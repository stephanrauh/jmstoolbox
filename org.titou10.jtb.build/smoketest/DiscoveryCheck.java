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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.titou10.jtb.jms.qm.DestinationDiscovery;

/** Regression checks for discovery timing and publication, independent of a broker or display. */
public class DiscoveryCheck {
   public static void main(String[] args) throws Exception {
      var arrivals = Executors.newSingleThreadScheduledExecutor();
      try {
         AtomicInteger refreshes = new AtomicInteger();
         try (var discovery = new DestinationDiscovery(refreshes::incrementAndGet, e -> {})) {
            long start = System.nanoTime();
            discovery.await(0, 500);
            check(elapsed(start) < 250, "zero timeout must return immediately");

            start = System.nanoTime();
            check(!discovery.await(100, 1000), "deadline must be reported as a timeout");
            check(elapsed(start) >= 80 && elapsed(start) < 1000, "timeout must cap the quiet period");

            // Notifications keep coming: each one must restart the quiet period.
            discovery.destinationsChanged();
            arrivals.schedule(discovery::destinationsChanged, 150, TimeUnit.MILLISECONDS);
            arrivals.schedule(discovery::destinationsChanged, 300, TimeUnit.MILLISECONDS);
            start = System.nanoTime();
            discovery.await(2000, 250);
            check(elapsed(start) >= 500, "quiet period must restart on late arrivals");

            // Discovery must continue after a request has timed out or returned.
            int before = refreshes.get();
            for (int i = 0; i < 2000; i++) {
               discovery.destinationsChanged();
            }
            discovery.await(2000, 200);
            int batches = refreshes.get() - before;
            check(batches > 0 && batches < 10, "advisory bursts must be batched");
         }

         // Silence must not return a stale snapshot while a refresh is still being published.
         CountDownLatch publishing = new CountDownLatch(1);
         CountDownLatch release = new CountDownLatch(1);
         try (var discovery = new DestinationDiscovery(() -> {
            publishing.countDown();
            release.await();
         }, e -> {})) {
            check(publishing.await(2, TimeUnit.SECONDS), "refresh did not start");
            arrivals.schedule(release::countDown, 250, TimeUnit.MILLISECONDS);
            long start = System.nanoTime();
            discovery.await(2000, 10);
            check(elapsed(start) >= 200, "wait must include publication of pending changes");
         } finally {
            release.countDown();
         }

         try (var discovery = new DestinationDiscovery(() -> {
            throw new IllegalStateException("refresh failed");
         }, e -> {})) {
            try {
               discovery.await(2000, 250);
               throw new AssertionError("refresh failures must reach waiting callers");
            } catch (IllegalStateException expected) {
               check(expected.getMessage().equals("refresh failed"), "unexpected failure");
            }
         }

         AtomicInteger attempts = new AtomicInteger();
         CountDownLatch failureReported = new CountDownLatch(1);
         CountDownLatch finishFailureCallback = new CountDownLatch(1);
         try (var discovery = new DestinationDiscovery(() -> {
            if (attempts.incrementAndGet() == 1) {
               throw new IllegalStateException("transient refresh failure");
            }
         }, e -> {
            failureReported.countDown();
            try {
               finishFailureCallback.await();
            } catch (InterruptedException interrupted) {
               Thread.currentThread().interrupt();
            }
         })) {
            check(failureReported.await(2, TimeUnit.SECONDS), "failure was not reported");
            // Retry while the failing publisher is still unwinding; no new advisory arrives.
            arrivals.schedule(finishFailureCallback::countDown, 100, TimeUnit.MILLISECONDS);
            check(discovery.await(2000, 10), "next call must recover from a stale failure");
            check(attempts.get() == 2, "a transient failure must be retried once");
            Thread.sleep(300);
            check(attempts.get() == 2, "idle discovery must not keep refreshing");
         } finally {
            finishFailureCallback.countDown();
         }

         var discovery = new DestinationDiscovery(() -> {}, e -> {});
         arrivals.schedule(discovery::close, 100, TimeUnit.MILLISECONDS);
         long start = System.nanoTime();
         discovery.await(5000, 5000);
         check(elapsed(start) < 1000, "disconnect must wake waiting callers");
         discovery.destinationsChanged(); // Late notifications after disconnect are harmless.
         discovery.close();
         System.out.println("PASS  asynchronous discovery timing, batching, publication, failure and shutdown");
      } finally {
         arrivals.shutdownNow();
      }
   }

   private static long elapsed(long start) {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
   }

   private static void check(boolean condition, String message) {
      if (!condition) {
         throw new AssertionError(message);
      }
   }
}
