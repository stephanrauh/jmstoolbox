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
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.management.remote.JMXConnector;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.activemq.ActiveMQConnection;
import org.slf4j.LoggerFactory;
import org.titou10.jtb.config.JTBPreferenceStore;
import org.titou10.jtb.config.MetaQManager;
import org.titou10.jtb.config.gen.Properties;
import org.titou10.jtb.config.gen.QManagerDef;
import org.titou10.jtb.config.gen.SessionDef;
import org.titou10.jtb.jms.model.JTBConnection;
import org.titou10.jtb.jms.model.JTBMessage;
import org.titou10.jtb.jms.model.JTBMessageType;
import org.titou10.jtb.jms.model.JTBQueue;
import org.titou10.jtb.jms.model.JTBSession;
import org.titou10.jtb.jms.model.JTBSessionClientType;
import org.titou10.jtb.jms.qm.DestinationData;
import org.titou10.jtb.qm.activemq.ActiveMQQManager;
import org.titou10.jtb.util.Constants;

/** Real broker connections, with faults injected only at discovery/cleanup boundaries. */
public class BrokerDiscoveryCheck {
   private static final JTBPreferenceStore PREFS = new JTBPreferenceStore();
   private static final HttpClient HTTP = HttpClient.newHttpClient();

   public static void main(String[] args) throws Exception {
      ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.ERROR);
      PREFS.setDefault(Constants.PREF_CONN_CLIENT_ID_PREFIX, "JTB-review");
      PREFS.setDefault(Constants.PREF_MAX_MESSAGES, 100);
      PREFS.setDefault(Constants.PREF_AUTO_RESIZE_COLS_BROWSER, false);
      recoveryAndSnapshots();
      readersDoNotWaitForRefresh();
      failedConnectClosesBothTransports();
      listenerDetachDuringAdvisories();
      concurrentShutdown();
      System.out.println("PASS  broker-backed discovery recovery, nonblocking snapshots, timeout warning, detach and shutdown");
   }

   private static void recoveryAndSnapshots() throws Exception {
      FaultManager manager = new FaultManager();
      JTBConnection connection = connection(manager, "review recovery", false);
      Logger logger = (Logger) LoggerFactory.getLogger(JTBConnection.class);
      Level oldLevel = logger.getLevel();
      ListAppender<ILoggingEvent> events = new ListAppender<>();
      events.start();
      logger.addAppender(events);
      logger.setLevel(Level.WARN);
      String queueName = "JTB.REVIEW." + System.nanoTime();
      try {
         connection.connect();
         var before = connection.getJtbQueues();
         check(before == connection.getJtbQueues(), "unchanged reads must reuse a snapshot");
         try {
            before.add(new JTBQueue(connection, "forbidden", null, true));
            throw new AssertionError("snapshot was mutable");
         } catch (UnsupportedOperationException expected) {
            // Expected: readers cannot corrupt the shared snapshot.
         }

         manager.failRefresh.set(true);
         jolokia("addQueue", queueName);
         check(manager.refreshFailed.await(5, TimeUnit.SECONDS), "real advisory did not trigger injected failure");
         try {
            connection.connect(2000, 10);
         } catch (IllegalStateException expected) {
            check(expected.getMessage().equals("injected refresh failure"), "unexpected refresh error");
         }
         // No new advisory: a subsequent request must recover on the same connection.
         connection.connect(2000, 10);
         check(manager.opened.size() == 1, "recovery must not reconnect");
         check(connection.getJTBDestinationByName(queueName) != null, "retry did not publish the new queue");
         check(before.stream().noneMatch(q -> q.getName().equals(queueName)), "old snapshot changed in place");
         var current = connection.getJtbQueues();
         check(current == connection.getJtbQueues(), "published snapshot was copied on read");

         JTBQueue queue = (JTBQueue) connection.getJTBDestinationByName(queueName);
         TextMessage text = (TextMessage) connection.createJMSMessage(JTBMessageType.TEXT);
         text.setText("after discovery recovery");
         connection.connect(); // Same path used by REST send/browse/empty.
         connection.sendMessage(new JTBMessage(queue, text));
         connection.connect();
         check(!connection.browseQueue(queue, 10).isEmpty(), "browse failed after refresh recovery");
         connection.connect();
         check(connection.emptyQueue(queue) == 1, "empty failed after refresh recovery");

         connection.connect(20, 60000);
         check(events.list.stream().anyMatch(e -> e.getLevel() == Level.WARN
               && e.getFormattedMessage().contains("may be incomplete")
               && e.getFormattedMessage().contains("review recovery")), "timeout warning is missing its session context");
      } finally {
         connection.disconnect();
         jolokia("removeQueue", queueName);
         logger.detachAppender(events);
         logger.setLevel(oldLevel);
         events.stop();
      }
   }

   private static void readersDoNotWaitForRefresh() throws Exception {
      FaultManager manager = new FaultManager();
      JTBConnection connection = new JTBConnection(PREFS, JTBSessionClientType.GUI,
               definition("review nonblocking reads", false), manager, null);
      String name = "JTB.REVIEW.READERS." + System.nanoTime();
      var reader = Executors.newSingleThreadExecutor();
      try {
         connection.connect(5000, 500);
         connection.updateFilterData("JTB.TEST", true);
         var queues = connection.getJtbQueues();
         var topics = connection.getJtbTopics();
         var filteredQueues = connection.getJtbQueuesFiltered();
         var filteredTopics = connection.getJtbTopicsFiltered();
         check(filteredQueues.size() == 1, "queue filter was not applied");
         manager.blockRefresh.set(true);
         jolokia("addQueue", name);
         check(manager.refreshEntered.await(5, TimeUnit.SECONDS), "refresh did not acquire the connection lock");
         reader.submit(() -> {
            check(connection.getJtbQueues() == queues, "queue snapshot changed before publication");
            check(connection.getJtbTopics() == topics, "topic snapshot changed before publication");
            check(connection.getJtbQueuesFiltered() == filteredQueues, "filtered queue snapshot was copied");
            check(connection.getJtbTopicsFiltered() == filteredTopics, "filtered topic snapshot was copied");
            check(connection.getJtbQueuesToDisplay() == filteredQueues, "displayed queue filter was ignored");
            check(connection.getJtbTopicsToDisplay() == filteredTopics, "displayed topic filter was ignored");
            check(connection.getJTBDestinationByName("JTB.TEST") != null, "destination lookup failed");
         }).get(1, TimeUnit.SECONDS);
      } finally {
         manager.releaseRefresh.countDown();
         reader.shutdownNow();
         connection.disconnect();
         jolokia("removeQueue", name);
      }
   }

   private static void failedConnectClosesBothTransports() throws Exception {
      FaultManager manager = new FaultManager();
      manager.failInitial = true;
      manager.failDetach = true;
      JTBConnection connection = connection(manager, "review failed connect", true);
      try {
         connection.connect();
         throw new AssertionError("initial discovery fault was ignored");
      } catch (IllegalStateException expected) {
         check(expected.getSuppressed().length == 1, "detach failure must be preserved");
      }
      check(!connection.isConnected() && manager.opened.get(0).isClosed(), "JMS transport leaked after detach failure");
      try {
         manager.jmx.getConnectionId();
         throw new AssertionError("JMX transport leaked after detach failure");
      } catch (java.io.IOException expected) {
         // JMX was closed even though detaching the advisory listener threw.
      }
      manager.failInitial = false;
      manager.failDetach = false;
      try {
         connection.connect();
         check(manager.opened.size() == 2 && manager.opened.get(0).isClosed(), "retry retained the previous connection");
      } finally {
         connection.disconnect();
      }
   }

   private static void listenerDetachDuringAdvisories() throws Exception {
      FaultManager manager = new FaultManager();
      ActiveMQConnection connection = (ActiveMQConnection) manager.connect(definition("review detach", false), false,
                                                                           "JTB-review-detach-" + System.nanoTime());
      var producer = Executors.newSingleThreadExecutor();
      String prefix = "JTB.REVIEW.DETACH." + System.nanoTime();
      try {
         var source = connection.getDestinationSource();
         var arrivals = producer.submit(() -> {
            for (int i = 0; i < 10; i++) {
               jolokia("addQueue", prefix + "." + i);
            }
            return null;
         });
         for (int i = 0; i < 2000; i++) {
            manager.setDestinationChangeListener(connection, () -> {});
            manager.setDestinationChangeListener(connection, null);
            check(source.getListener() != null, "detach exposed a null listener to ActiveMQ's dispatcher");
         }
         arrivals.get(10, TimeUnit.SECONDS);
         waitUntil(() -> source.getQueues().stream().filter(q -> q.getPhysicalName().startsWith(prefix)).count() == 10);
      } finally {
         producer.shutdownNow();
         manager.close(connection);
         for (int i = 0; i < 10; i++) {
            jolokia("removeQueue", prefix + "." + i);
         }
      }
   }

   private static void concurrentShutdown() throws Exception {
      FaultManager manager = new FaultManager();
      QManagerDef qm = new QManagerDef();
      qm.setId("review");
      MetaQManager meta = new MetaQManager(qm);
      meta.setQmanager(manager);
      JTBSession session = new JTBSession(PREFS, definition("review shutdown", false), meta);
      session.getJTBConnection(JTBSessionClientType.GUI).connect();
      session.getJTBConnection(JTBSessionClientType.REST).connect();
      manager.blockClose = true;
      var threads = Executors.newFixedThreadPool(2);
      CountDownLatch created = new CountDownLatch(1);
      try {
         var shutdown = threads.submit(session::disconnectAll);
         check(manager.closeEntered.await(5, TimeUnit.SECONDS), "shutdown did not start");
         var writer = threads.submit(() -> {
            session.getJTBConnection(JTBSessionClientType.SCRIPT);
            created.countDown();
         });
         check(!created.await(100, TimeUnit.MILLISECONDS), "connection registry changed during shutdown iteration");
         manager.releaseClose.countDown();
         shutdown.get(10, TimeUnit.SECONDS);
         writer.get(10, TimeUnit.SECONDS);
         check(manager.opened.stream().allMatch(ActiveMQConnection::isClosed), "shutdown left an existing JMS connection open");
      } finally {
         manager.releaseClose.countDown();
         threads.shutdownNow();
         session.disconnectAll();
      }
   }

   private static JTBConnection connection(FaultManager manager, String name, boolean jmx) {
      return new JTBConnection(PREFS, JTBSessionClientType.REST, definition(name, jmx), manager, null);
   }

   private static SessionDef definition(String name, boolean jmx) {
      SessionDef def = new SessionDef();
      def.setName(name);
      def.setHost("localhost");
      def.setPort(1099);
      def.setActiveUserid("admin");
      def.setActivePassword("admin");
      Properties properties = new Properties();
      for (String[] item : new String[][] {{"brokerURL", "tcp://localhost:61617"}, {"useJMX", "" + jmx}, {"jmxContext", "jmxrmi"}}) {
         Properties.Property property = new Properties.Property();
         property.setName(item[0]);
         property.setValue(item[1], false);
         properties.getProperty().add(property);
      }
      def.setProperties(properties);
      return def;
   }

   private static void jolokia(String operation, String name) throws Exception {
      String body = "{\"type\":\"exec\",\"mbean\":\"org.apache.activemq:type=Broker,brokerName=localhost\","
                    + "\"operation\":\"" + operation + "\",\"arguments\":[\"" + name + "\"]}";
      var request = HttpRequest.newBuilder(URI.create("http://localhost:8162/api/jolokia/"))
               .timeout(Duration.ofSeconds(10)).header("Authorization", "Basic YWRtaW46YWRtaW4=")
               .header("Origin", "http://localhost").header("Content-Type", "application/json")
               .POST(HttpRequest.BodyPublishers.ofString(body)).build();
      String response = HTTP.send(request, HttpResponse.BodyHandlers.ofString()).body();
      check(response.contains("\"status\":200"), "Jolokia failed: " + response);
   }

   private static void waitUntil(BooleanSupplier condition) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!condition.getAsBoolean()) {
         check(System.nanoTime() < deadline, "advisories did not arrive");
         Thread.sleep(20);
      }
   }

   private static void check(boolean condition, String message) {
      if (!condition) {
         throw new AssertionError(message);
      }
   }

   private static class FaultManager extends ActiveMQQManager {
      final AtomicBoolean failRefresh = new AtomicBoolean();
      final AtomicBoolean blockRefresh = new AtomicBoolean();
      final CountDownLatch refreshEntered = new CountDownLatch(1);
      final CountDownLatch releaseRefresh = new CountDownLatch(1);
      final CountDownLatch refreshFailed = new CountDownLatch(1);
      final CopyOnWriteArrayList<ActiveMQConnection> opened = new CopyOnWriteArrayList<>();
      final CountDownLatch closeEntered = new CountDownLatch(1);
      final CountDownLatch releaseClose = new CountDownLatch(1);
      boolean failInitial;
      boolean failDetach;
      boolean blockClose;
      JMXConnector jmx;

      @Override
      public Connection connect(SessionDef definition, boolean showSystemObjects, String clientID) throws Exception {
         Connection connection = super.connect(definition, showSystemObjects, clientID);
         opened.add((ActiveMQConnection) connection);
         Field sessions = ActiveMQQManager.class.getDeclaredField("sessionsInfo");
         sessions.setAccessible(true);
         Object info = ((Map<?, ?>) sessions.get(this)).get(connection.hashCode());
         var accessor = info.getClass().getDeclaredMethod("getJmxc");
         accessor.setAccessible(true);
         jmx = (JMXConnector) accessor.invoke(info);
         return connection;
      }

      @Override
      public DestinationData discoverDestinations(Connection connection, boolean showSystemObjects) throws Exception {
         if (failInitial) {
            throw new IllegalStateException("injected initial discovery failure");
         }
         if (blockRefresh.compareAndSet(true, false)) {
            refreshEntered.countDown();
            check(releaseRefresh.await(5, TimeUnit.SECONDS), "blocked refresh was not released");
         }
         if (failRefresh.compareAndSet(true, false)) {
            refreshFailed.countDown();
            throw new IllegalStateException("injected refresh failure");
         }
         return super.discoverDestinations(connection, showSystemObjects);
      }

      @Override
      public boolean setDestinationChangeListener(Connection connection, Runnable listener) throws Exception {
         if (listener == null && failDetach) {
            throw new IllegalStateException("injected detach failure");
         }
         return super.setDestinationChangeListener(connection, listener);
      }

      @Override
      public void close(Connection connection) throws JMSException {
         if (blockClose) {
            closeEntered.countDown();
            try {
               check(releaseClose.await(5, TimeUnit.SECONDS), "close was not released");
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new JMSException("interrupted shutdown test");
            }
         }
         super.close(connection);
      }
   }
}
