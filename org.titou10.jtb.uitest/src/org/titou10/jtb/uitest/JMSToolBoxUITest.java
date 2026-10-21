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
package org.titou10.jtb.uitest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.titou10.jtb.uitest.UITestReport.step;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.StreamMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.exceptions.WidgetNotFoundException;
import org.eclipse.swtbot.swt.finder.finders.UIThreadRunnable;
import org.eclipse.swtbot.swt.finder.utils.SWTBotPreferences;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTabItem;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTree;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTreeItem;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;

/**
 * UI tests of JMSToolBox, run in the real application by Tycho with SWTBot.
 *
 * The sessions come from org.titou10.jtb.build/smoketest/config.xml, the brokers from smoketest/docker-compose.yml. All tests
 * use the queue JTB.TEST of the ActiveMQ Classic session and empty it first
 *
 */
public class JMSToolBoxUITest {

   private static final String SESSION          = "ActiveMQ Classic";
   private static final String SESSION_ARTEMIS2 = "Artemis 2.x";
   private static final String QUEUE            = "JTB.TEST";
   private static final String VISUALIZER       = "SWTBot visualizer";

   // REST API (Jolokia) of the ActiveMQ Classic broker, used to prepare the queue
   private static final String JOLOKIA_URL      = "http://localhost:8162/api/jolokia/";
   private static final String QUEUE_MBEAN      = "org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName="
                                                  + QUEUE;

   private static SWTBot       bot;

   // Records the result of each test in the report (see UITestReport)
   @Rule
   public TestWatcher          report           = UITestReport.watcher();

   @BeforeClass
   public static void setUp() {
      SWTBotPreferences.TIMEOUT = 30_000;
      // Pause between SWTBot actions, to watch the tests (uitest.sh --slow)
      SWTBotPreferences.PLAYBACK_DELAY = Long.getLong("uitest.delay", 0);
      bot = new SWTBot();
      step("JMSToolBox started");
   }

   // Closes the dialogs a failed test may have left open, so that they don't disturb the next tests
   @Before
   public void closeLeftoverDialogs() {
      UIThreadRunnable.syncExec(() -> {
         for (Shell shell : Display.getDefault().getShells()) {
            if (shell.getParent() != null && shell.isVisible()) {
               shell.close();
            }
         }
      });
   }

   @AfterClass
   public static void writeReport() throws Exception {
      UITestReport.write();
   }

   @Test
   public void connectSessionAndListQueues() {
      SWTBotTreeItem session = connect(SESSION);

      SWTBotTreeItem queues = session.getNode("Queues").expand();
      step("Expanded the queues of " + SESSION + ": " + queues.getNodes());
      assertTrue("Queue " + QUEUE + " not found in " + queues.getNodes(), queues.getNodes().contains(QUEUE));
   }

   @Test
   public void sendMessageWithDialog() throws Exception {
      String payload = uniquePayload("sent with the dialog");
      purgeQueue();

      // Right-click on the queue > "Send New Message..."
      connect(SESSION).getNode("Queues").expand().getNode(QUEUE).contextMenu("Send New Message...").click();
      SWTBotShell dialog = bot.shell("Send Message to " + SESSION + ":" + QUEUE);
      SWTBot dialogBot = dialog.bot();
      step("Opened the dialog '" + dialog.getText() + "'");

      // Payload tab: the multi-line text field of the payload
      inTab(dialogBot, "Payload").text(0).setText(payload);
      // Properties tab: name and value of a new property, then "Add"
      SWTBot properties = inTab(dialogBot, "Properties");
      properties.text(0).setText("jtbUiTest");
      properties.text(1).setText("dialog");
      properties.button("Add").click();
      step("Entered the payload and the property jtbUiTest=dialog");

      dialogBot.button("Send and Close").click();
      step("Clicked 'Send and Close'");

      // The message shows up when browsing the queue, with the property
      browseQueueUntilShown(payload);
      assertEquals("dialog", valueInTable(inTab(bot, "Properties").table(), "jtbUiTest"));
      step("Browsed the queue: the message has the payload and the property jtbUiTest=dialog");
   }

   @Test
   public void showMessageDetails() throws Exception {
      String payload = uniquePayload("details");
      String correlationId = "corr-" + System.currentTimeMillis();
      purgeQueue();
      jolokia("{\"type\":\"exec\",\"mbean\":\"" + QUEUE_MBEAN + "\",\"operation\":\"sendTextMessage(java.util.Map,java.lang.String)\","
              + "\"arguments\":[{\"JMSCorrelationID\":\"" + correlationId + "\",\"jtbUiTest\":\"details\"},\"" + payload + "\"]}");

      browseQueueUntilShown(payload);
      step("Selected the message: toString shows the payload");

      assertEquals(correlationId, valueInTable(inTab(bot, "JMS Headers").table(), "JMSCorrelationID"));
      step("JMS Headers tab: JMSCorrelationID=" + correlationId);

      assertEquals("details", valueInTable(inTab(bot, "Properties").table(), "jtbUiTest"));
      step("Properties tab: jtbUiTest=details");
   }

   @Test
   public void openMessageWithJavaScriptVisualizer() throws Exception {
      String payload = uniquePayload("visualizer");
      purgeQueue();
      jolokia("{\"type\":\"exec\",\"mbean\":\"" + QUEUE_MBEAN
              + "\",\"operation\":\"sendTextMessage(java.lang.String)\",\"arguments\":[\"" + payload + "\"]}");

      SWTBotTable messages = browseQueueUntilShown(payload);

      // Run the inline JavaScript visualizer (workspace/visualizers.xml) on the message
      messages.getTableItem(0).select().contextMenu("Open Payload as...").menu(VISUALIZER).click();

      SWTBotShell logs = bot.shell("Script logs");
      step("Clicked 'Open Payload as... > " + VISUALIZER + "' on the message: dialog '" + logs.getText() + "' opened");
      String expected = "SWTBOT:" + payload.toUpperCase();
      bot.waitUntil(new DefaultCondition() {
         @Override
         public boolean test() {
            return logs.bot().text().getText().contains(expected);
         }

         @Override
         public String getFailureMessage() {
            return "'" + expected + "' not found in the script logs: " + logs.bot().text().getText();
         }
      });
      step("Script logs show: " + logs.bot().text().getText().trim());
      logs.close();
   }

   // ---------------------------------------------------------------------------------------------------------
   // Message viewer: every JMS message type, shown in the details below the message table and in the dialog that
   // opens on double-click. The object message contains a plain JDK object (java.util.ArrayList)
   // ---------------------------------------------------------------------------------------------------------

   @Test
   public void viewTextMessage() throws Exception {
      viewMessage("TEXT", "ActiveMQTextMessage", s -> s.createTextMessage("swtbot text message"));
   }

   @Test
   public void viewBytesMessage() throws Exception {
      viewMessage("BYTES", "ActiveMQBytesMessage", s -> {
         BytesMessage m = s.createBytesMessage();
         m.writeBytes("swtbot bytes message".getBytes(StandardCharsets.UTF_8));
         return m;
      });
   }

   @Test
   public void viewMapMessage() throws Exception {
      viewMessage("MAP", "ActiveMQMapMessage", s -> {
         MapMessage m = s.createMapMessage();
         m.setString("text", "swtbot map message");
         m.setInt("number", 42);
         return m;
      });
   }

   // The ActiveMQ client only deserializes classes of its trusted packages (by default org.apache.activemq,
   // org.fusesource.hawtbuf, com.thoughtworks.xstream.mapper), unless the session sets "trustAllPackages". So the payload of
   // this object message (a java.util.ArrayList) can't be read. Today, JMSToolBox shows the details of the message, logs the
   // error and doesn't open the dialog on double-click
   @Test
   public void viewObjectMessageWithUntrustedPayload() throws Exception {
      purgeQueue();
      String id = sendWithJms(s -> s.createObjectMessage(new ArrayList<>(List.of("swtbot", "object", "message"))));

      SWTBotTable messages = browseQueueUntilShown(id);
      String toString = inTab(bot, "toString").text(0).getText();
      assertTrue("The toString doesn't mention ActiveMQObjectMessage: " + toString, toString.contains("ActiveMQObjectMessage"));
      step("Details of the OBJECT message (ActiveMQObjectMessage)");

      messages.getTableItem(0).doubleClick();
      bot.sleep(3000);
      for (SWTBotShell shell : bot.shells()) {
         assertFalse("Unexpected dialog '" + shell.getText() + "'", shell.getText().contains("OBJECT"));
      }
      assertTrue("The error reading the payload is not in the log", appLog().contains("when reading Object Payload"));
      step("Double-clicked the OBJECT message: no dialog, the error reading the payload is logged");
   }

   // A JSON payload is shown formatted in the "Payload (JSON)" tab and as a tree in "Payload (JSON Tree)" (jakarta.json)
   @Test
   public void viewJsonMessage() throws Exception {
      String json = "{\"order\":{\"id\":4711,\"customer\":\"swtbot\",\"items\":[\"json\",\"tree\"]}}";
      purgeQueue();
      String id = sendWithJms(s -> s.createTextMessage(json));

      browseQueueUntilShown(id);
      String formatted = inTab(bot, "Payload (JSON)").text(0).getText();
      assertTrue("The JSON is not formatted: " + formatted, formatted.lines().count() > 3 && formatted.contains("\"customer\""));
      step("Payload (JSON) tab: the payload is formatted");

      List<String> nodes = allItemTexts(inTab(bot, "Payload (JSON Tree)").tree());
      String all = String.join(" | ", nodes);
      assertTrue("The JSON tree doesn't contain the payload: " + all, all.contains("customer") && all.contains("swtbot")
                                                                       && all.contains("4711"));
      step("Payload (JSON Tree) tab: " + all);
   }

   // Saves a message as template (JAXB writes the template file) and sends a new message from it (JAXB reads it back)
   @Test
   public void saveAsTemplateAndSendFromTemplate() throws Exception {
      String payload = uniquePayload("template");
      String templateName = "swtbot-" + System.currentTimeMillis();
      purgeQueue();
      sendWithJms(s -> s.createTextMessage(payload));

      // Double-click on the message > "Save as Template..."
      browseQueueUntilShown(payload).getTableItem(0).doubleClick();
      SWTBotShell view = waitForShellWithTitleContaining("TEXT");
      view.bot().button("Save as Template...").click();
      SWTBotShell save = bot.shell("Save Template");
      save.bot().text(0).setText(templateName);
      step("Clicked 'Save as Template...' and entered the name " + templateName);
      save.bot().button("OK").click();

      // The template is an XML file; the payload is stored Base64 encoded with the prefix {##}
      Path file = Path.of(System.getProperty("uitest.workspace", "target/workspace"), "JMSToolBox", "Templates",
                          templateName + ".jtb");
      String encodedPayload = "{##}" + Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
      bot.waitUntil(new DefaultCondition() {
         @Override
         public boolean test() throws Exception {
            return Files.exists(file) && Files.readString(file, StandardCharsets.UTF_8).contains(encodedPayload);
         }

         @Override
         public String getFailureMessage() {
            return "Template file " + file + " not written or without the payload";
         }
      });
      step("Template file " + file.getFileName() + " written, it contains the payload");

      // Right-click on the queue > "Send Message from Template..." > select the template > "Send"
      purgeQueue();
      connect(SESSION).getNode("Queues").expand().getNode(QUEUE).contextMenu("Send Message from Template...").click();
      SWTBotShell chooser = bot.shell("Select a Template");
      selectTreeItemContaining(chooser.bot().tree(), templateName);
      step("Clicked 'Send Message from Template...' and selected the template");
      chooser.bot().button("OK").click();
      SWTBotShell send = bot.shell("Send Message to " + SESSION + ":" + QUEUE);
      step("Dialog '" + send.getText() + "' opened with the template");
      send.bot().button("Send").click();

      browseQueueUntilShown(payload);
      step("Browsed the queue: the message sent from the template has the payload");
   }

   // ----------------------------------------------------
   // Artemis 2.x
   // ----------------------------------------------------

   @Test
   public void connectArtemis2SessionAndListQueues() {
      SWTBotTreeItem session = connect(SESSION_ARTEMIS2);

      SWTBotTreeItem queues = session.getNode("Queues").expand();
      step("Expanded the queues of " + SESSION_ARTEMIS2 + ": " + queues.getNodes());
      assertTrue("Queue " + QUEUE + " not found in " + queues.getNodes(), queues.getNodes().contains(QUEUE));
   }

   @Test
   public void viewStreamMessage() throws Exception {
      viewMessage("STREAM", "ActiveMQStreamMessage", s -> {
         StreamMessage m = s.createStreamMessage();
         m.writeString("swtbot stream message");
         m.writeInt(42);
         return m;
      });
   }

   @Test
   public void viewMessageWithoutPayload() throws Exception {
      viewMessage("MESSAGE", "ActiveMQMessage", Session::createMessage);
   }

   // Sends a message of the given type, browses the queue, checks its details and opens it with a double-click
   private static void viewMessage(String type, String jmsClass, MessageFactory factory) throws Exception {
      purgeQueue();
      String id = sendWithJms(factory);

      SWTBotTable messages = browseQueueUntilShown(id);
      String toString = inTab(bot, "toString").text(0).getText();
      assertTrue("The toString of the " + type + " message doesn't mention " + jmsClass + ": " + toString,
                 toString.contains(jmsClass));
      step("Details of the " + type + " message (" + jmsClass + ")");

      messages.getTableItem(0).doubleClick();
      SWTBotShell dialog = waitForShellWithTitleContaining(type);
      step("Double-clicked the " + type + " message: dialog '" + dialog.getText() + "' opened");
      // The name of the payload tab depends on the payload, eg "Payload: 20 bytes"
      activateTabStartingWith(dialog.bot(), "Payload");
      step("Payload tab of the " + type + " message");
      dialog.close();
   }

   private static void activateTabStartingWith(SWTBot parent, String text) {
      for (int i = 0; i < 10; i++) {
         SWTBotTabItem tab = parent.tabItem(i);
         if (tab.getText().startsWith(text)) {
            tab.activate();
            return;
         }
      }
   }

   @FunctionalInterface
   private interface MessageFactory {
      Message create(Session session) throws JMSException;
   }

   // Sends a message with the JMS client of ActiveMQ. Returns the value of its property "jtbUiTestId", which its toString
   // contains, so that browseQueueUntilShown() can recognize it
   private static String sendWithJms(MessageFactory factory) throws Exception {
      String id = "swtbot-" + System.nanoTime();
      var cf = new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:61617");
      try (Connection connection = cf.createConnection()) {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Message message = factory.create(session);
         message.setStringProperty("jtbUiTestId", id);
         session.createProducer(session.createQueue(QUEUE)).send(message);
      }
      return id;
   }

   private static SWTBotShell waitForShellWithTitleContaining(String text) {
      SWTBotShell[] found = new SWTBotShell[1];
      bot.waitUntil(new DefaultCondition() {
         @Override
         public boolean test() {
            for (SWTBotShell shell : bot.shells()) {
               if (shell.getText().contains(text)) {
                  found[0] = shell;
                  return true;
               }
            }
            return false;
         }

         @Override
         public String getFailureMessage() {
            return "No window with '" + text + "' in its title";
         }
      });
      return found[0];
   }

   // -------
   // Helpers
   // -------

   // Double clicks on the session if it is not connected yet, and waits until its "Queues" folder shows up
   private static SWTBotTreeItem connect(String sessionName) {
      SWTBotTreeItem session = sessionsTree().getTreeItem(sessionName);
      if (!session.expand().getNodes().contains("Queues")) {
         session.doubleClick();
         step("Double-clicked session " + sessionName);
      }
      bot.waitUntil(new DefaultCondition() {
         @Override
         public boolean test() {
            return session.expand().getNodes().contains("Queues");
         }

         @Override
         public String getFailureMessage() {
            return "Session '" + sessionName + "' did not connect. Nodes: " + session.getNodes();
         }
      });
      step("Session " + sessionName + " connected: " + session.getNodes());
      return session;
   }

   // Double clicks on the queue, waits until the table shows exactly one message, selects it and waits until its toString
   // contains the payload. Waiting for the payload makes sure that it's not the table of a previous test
   private static SWTBotTable browseQueueUntilShown(String payload) {
      connect(SESSION).getNode("Queues").expand().getNode(QUEUE).doubleClick();
      SWTBotTable[] found = new SWTBotTable[1];
      bot.waitUntil(new DefaultCondition() {
         @Override
         public boolean test() {
            SWTBotTable table = messageTable();
            if (table == null || table.rowCount() != 1) {
               return false;
            }
            table.select(0);
            if (!inTab(bot, "toString").text(0).getText().contains(payload)) {
               return false;
            }
            found[0] = table;
            return true;
         }

         @Override
         public String getFailureMessage() {
            return "Message '" + payload + "' not shown when browsing " + QUEUE;
         }
      });
      step("Double-clicked queue " + QUEUE + " and selected the message '" + payload + "'");
      return found[0];
   }

   // The sessions browser is one of several trees in the window: find the one containing the session.
   // The widgets are searched directly: bot.tree(index) waits SWTBotPreferences.TIMEOUT for each index that doesn't exist
   private static SWTBotTree sessionsTree() {
      Tree tree = findWidget(Tree.class, t -> Arrays.stream(t.getItems()).anyMatch(i -> i.getText().equals(SESSION)));
      if (tree == null) {
         throw new WidgetNotFoundException("No tree contains the session '" + SESSION + "'");
      }
      return new SWTBotTree(tree);
   }

   // The table of the messages of a queue, recognized by its "JMS Timestamp" column. null if not shown (yet)
   private static SWTBotTable messageTable() {
      Table table = findWidget(Table.class,
                               t -> Arrays.stream(t.getColumns()).anyMatch(c -> c.getText().equals("JMS Timestamp")));
      return table == null ? null : new SWTBotTable(table);
   }

   // First widget of the given type in any open window that matches the condition, without waiting. null if none
   private static <T extends Widget> T findWidget(Class<T> type, Predicate<T> condition) {
      return UIThreadRunnable.syncExec(() -> {
         Deque<Control> controls = new ArrayDeque<>(List.of(Display.getDefault().getShells()));
         while (!controls.isEmpty()) {
            Control control = controls.pop();
            if (control.isDisposed()) {
               continue;
            }
            if (type.isInstance(control) && condition.test(type.cast(control))) {
               return type.cast(control);
            }
            if (control instanceof Composite composite) {
               controls.addAll(List.of(composite.getChildren()));
            }
         }
         return null;
      });
   }

   // Activates a tab and returns a bot that only finds the widgets of that tab
   private static SWTBot inTab(SWTBot parent, String tabText) {
      SWTBotTabItem tab = parent.tabItem(tabText).activate();
      Control control = UIThreadRunnable.syncExec(() -> tab.widget.getControl());
      return new SWTBot(control);
   }

   // Value in the second column of the row whose first column is the given name (tables "Header | Value")
   private static String valueInTable(SWTBotTable table, String name) {
      for (int row = 0; row < table.rowCount(); row++) {
         if (name.equals(table.cell(row, 0))) {
            return table.cell(row, 1);
         }
      }
      throw new AssertionError("'" + name + "' not found in the table");
   }

   // Texts of all items of a tree, at all levels
   private static List<String> allItemTexts(SWTBotTree tree) {
      return UIThreadRunnable.syncExec(() -> {
         List<String> texts = new ArrayList<>();
         Deque<TreeItem> items = new ArrayDeque<>(List.of(tree.widget.getItems()));
         while (!items.isEmpty()) {
            TreeItem item = items.pop();
            texts.add(item.getText());
            items.addAll(List.of(item.getItems()));
         }
         return texts;
      });
   }

   // Expands the tree and selects the first item whose text contains the given text
   private static void selectTreeItemContaining(SWTBotTree tree, String text) {
      for (SWTBotTreeItem item : tree.getAllItems()) {
         item.expand();
         for (String node : item.getNodes()) {
            if (node.contains(text)) {
               item.getNode(node).select();
               return;
            }
         }
      }
      throw new WidgetNotFoundException("No item '" + text + "' in the tree");
   }

   // The log of JMSToolBox, in the workspace of the UI tests
   private static String appLog() throws Exception {
      Path log = Path.of(System.getProperty("uitest.workspace", "target/workspace"), "JMSToolBox", "jmstoolbox.log");
      return Files.readString(log, StandardCharsets.UTF_8);
   }

   private static String uniquePayload(String text) {
      return "swtbot " + text + " " + System.currentTimeMillis();
   }

   private static void purgeQueue() throws Exception {
      jolokia("{\"type\":\"exec\",\"mbean\":\"" + QUEUE_MBEAN + "\",\"operation\":\"purge\"}");
   }

   private static void jolokia(String request) throws Exception {
      String auth = Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8));
      HttpRequest r = HttpRequest.newBuilder(URI.create(JOLOKIA_URL)).header("Authorization", "Basic " + auth)
               .header("Origin", "http://localhost").header("Content-Type", "application/json")
               .POST(HttpRequest.BodyPublishers.ofString(request)).build();
      HttpResponse<String> response = HttpClient.newHttpClient().send(r, HttpResponse.BodyHandlers.ofString());
      if (!response.body().contains("\"status\":200")) {
         throw new IllegalStateException("Jolokia request failed: " + response.body());
      }
   }
}
