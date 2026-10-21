/*
 * Copyright (C) 2026 Stephan Rauh
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.titou10.jtb.jms.qm;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Batches advisory notifications and lets callers wait for a quiet, published snapshot.
 * <p>
 * State, all guarded by this monitor:
 * <ul>
 * <li>{@code revision} is incremented by every advisory. {@code publishedRevision} is the revision the last successful
 * refresh saw. The snapshot is current when both are equal.</li>
 * <li>{@code lastChange} is the time of the last advisory. {@link #await} waits until it is a quiet period old.</li>
 * <li>{@code scheduled} means a {@link #publish} is queued or running. There is at most one, so a burst of advisories
 * costs one refresh, and no timer runs while idle.</li>
 * <li>{@code failure} is the exception of the last refresh. It is cleared when a publish starts, or when {@link #await}
 * asks for a retry.</li>
 * <li>{@code closed} is final: every method becomes a no-op and waiters return.</li>
 * </ul>
 * The refresh runs without holding this monitor, because it takes the owner's lock ({@code JTBConnection}). Never hold
 * this monitor while taking the owner's lock, or the two can deadlock.
 */
public final class DestinationDiscovery implements AutoCloseable {

   public static final long DEFAULT_TIMEOUT_MS = 5000;
   public static final long DEFAULT_QUIET_PERIOD_MS = 500;

   @FunctionalInterface
   public interface Refresh {
      void run() throws Exception;
   }

   private final ScheduledExecutorService executor;
   private final Refresh refresh;
   private final Consumer<Exception> onFailure;
   private long revision = 1;
   private long publishedRevision;
   private long lastChange = System.nanoTime();
   private boolean scheduled;
   private boolean closed;
   private Exception failure;

   public DestinationDiscovery(Refresh refresh, Consumer<Exception> onFailure) {
      this.refresh = refresh;
      this.onFailure = onFailure;
      executor = Executors.newSingleThreadScheduledExecutor(task -> {
         Thread thread = new Thread(task, "JTB destination discovery");
         thread.setDaemon(true);
         return thread;
      });
      synchronized (this) {
         schedulePublish();
      }
   }

   public synchronized void destinationsChanged() {
      if (!closed) {
         revision++;
         lastChange = System.nanoTime();
         schedulePublish();
         notifyAll();
      }
   }

   // All callers hold this monitor. A burst schedules one publication, with no timer running when idle.
   private void schedulePublish() {
      if (!closed && !scheduled) {
         scheduled = true;
         executor.schedule(this::publish, 100, TimeUnit.MILLISECONDS);
      }
   }

   private void publish() {
      long currentRevision;
      synchronized (this) {
         if (closed) {
            return;
         }
         currentRevision = revision;
         failure = null;
      }
      try {
         refresh.run();
         synchronized (this) {
            publishedRevision = currentRevision;
         }
      } catch (Exception e) {
         synchronized (this) {
            if (closed) {
               return;
            }
            failure = e;
         }
         onFailure.accept(e);
      } finally {
         synchronized (this) {
            scheduled = false;
            // Publish again when
            // - advisories arrived while the refresh ran (revision moved on), or
            // - await() cleared a failure while this attempt was still running. Its schedulePublish() did nothing,
            //   because scheduled was still true.
            // A failure nobody cleared is not retried here. The next advisory or await() retries it.
            if (revision != currentRevision || (publishedRevision != revision && failure == null)) {
               schedulePublish();
            }
            notifyAll();
         }
      }
   }

   public static void validateTimes(long timeoutMs, long quietPeriodMs) {
      if (timeoutMs < 0 || quietPeriodMs < 0) {
         throw new IllegalArgumentException("Discovery times must be non-negative");
      }
   }

   /** Returns false on timeout. Silence is a heuristic, not proof of a complete initial list. */
   public synchronized boolean await(long timeoutMs, long quietPeriodMs) throws Exception {
      validateTimes(timeoutMs, quietPeriodMs);
      if (timeoutMs == 0 || quietPeriodMs == 0) {
         return true;
      }
      // A previous failure must not poison an otherwise connected session. Retry even without a new advisory.
      if (failure != null) {
         failure = null;
         schedulePublish();
      }
      long start = System.nanoTime();
      long timeout = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
      long quietPeriod = TimeUnit.MILLISECONDS.toNanos(quietPeriodMs);
      while (!closed) {
         if (failure != null) {
            throw failure;
         }
         long now = System.nanoTime();
         long remaining = timeout - (now - start);
         long quietRemaining = quietPeriod - (now - lastChange);
         if (quietRemaining <= 0 && publishedRevision == revision) {
            return true;
         }
         if (remaining <= 0) {
            return false;
         }
         TimeUnit.NANOSECONDS.timedWait(this, quietRemaining > 0 ? Math.min(remaining, quietRemaining) : remaining);
      }
      return true;
   }

   @Override
   public synchronized void close() {
      closed = true;
      executor.shutdownNow();
      notifyAll();
   }
}
