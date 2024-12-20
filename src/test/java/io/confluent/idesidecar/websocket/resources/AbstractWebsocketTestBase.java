package io.confluent.idesidecar.websocket.resources;

import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.confluent.idesidecar.websocket.messages.HelloBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.quarkus.logging.Log;
import io.quarkus.test.common.http.TestHTTPResource;
import jakarta.inject.Inject;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for unit tests that involve websocket connections.
 */
abstract class AbstractWebsocketTestBase {

  /**
   * A websocket client message handler that stores messages received into a LinkedBlockingDeque<Message> on
   * behalf of a test. The test can then interact with LinkedBlockingDeque<Message> messages.
   */
  public static class TestWebsocketClientMessageHandler implements MessageHandler.Whole<String> {

    // Where this client will store deserialized messages received.
    private final LinkedBlockingDeque<Message> messages = new LinkedBlockingDeque<>();

    // likewise, but for the raw message json strings. Needed for tests which send explictly
    // unknown message types whose serialization will coerce to DynamicMessageBody + MessageTypes.UNKNOWN,
    // but when delivered through to other workspaces, the original message type should be preserved.
    private final LinkedBlockingDeque<String> rawMessageStrings = new LinkedBlockingDeque<>();

    // For deserializing messages.
    @Override
    public void onMessage(String messageString) {
      Log.info("Test client received message: " + messageString);
      rawMessageStrings.add(messageString);
      try {
        // Deserialize json -> Message, add to our list of messages so that the main
        // test can check them.
        Message message = MAPPER.readValue(messageString, Message.class);
        messages.add(message);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public Message poll(Duration duration) {
      try {
        return messages.poll(duration.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Empty out any received messages
     */
    public void clear() {
      messages.clear();
      rawMessageStrings.clear();
    }
  }

  /**
   * A websocket client configurator that sets the access token in the headers. This is a
   * requirement for us --- AccessTokeFilter guards the /ws route.
   */
  public static class TestWebsocketClientConfigurator extends ClientEndpointConfig.Configurator {

    /**
     * The access token to embed in the ws connect GET route hit.
     * Will be assigned once the handshake is done. Must be static
     * because we don't have control over when the instances of
     * this class are created, sigh.
     * */
    private static String accessToken = "";

    public static void setAccessToken(String token) {
      accessToken = token;
    }

    public TestWebsocketClientConfigurator() {
    }

    /**
     * Add the access token to the headers used in the client-side GET request starting a websocket
     * connection.
     */
    @Override
    public void beforeRequest(Map<String, List<String>> headers) {
      headers.put("Authorization", List.of("Bearer " + accessToken));
    }
  }

  /**
   * A websocket client that connects to the websocket endpoint.
   * I can't find a way to control the construction of this class when needing to use
   * the TestWebsocketClientConfigurator, so the real work assisting the test
   * is being done by the TestWebsocketClientMessageHandler instances thar are
   * correlated with each of these clients.
   */
  @ClientEndpoint(configurator = TestWebsocketClientConfigurator.class)
  public static class TestWebsocketClient{
  }

  /**
   * Finally, a convenience bundle of the above:
   * 1. a mock workspace process,
   * 2. its websocket session,
   * 3. and the message handler that will store messages received by the client.
   * See {@link #connectWorkspace}
   */
  protected record ConnectedWorkspace (
      MockWorkspaceProcess mockWorkspaceProcess,
      Session session,
      TestWebsocketClientMessageHandler messageHandler
  ) {

    public WorkspacePid processId() {
      return mockWorkspaceProcess.pid;
    }

    /**
     * Send a HELLO message to the websocket endpoint. Do not wait for a response.
     */
    public void sayHello() {
      sayHello(null, null);
    }

    /**
     * Send a HELLO message to the websocket endpoint. Do not wait for a response.
     * Caller can provide an alternate pid to hello with if so desired, for either
     * the pid spelled in general message header, or in the body of the hello.
     * Defaults to the pid of the mock workspace process.
     */
    public void sayHello(
        WorkspacePid headerPid,
        WorkspacePid bodyPid
    ) {
      headerPid = headerPid != null ? headerPid : mockWorkspaceProcess.pid;
      bodyPid = bodyPid != null ? bodyPid : mockWorkspaceProcess.pid;

      var helloMessage = new Message(
          new MessageHeaders(
              MessageType.WORKSPACE_HELLO, headerPid.toString(),
              "message-id-here"),
          new HelloBody(bodyPid.id())
      );
      send(helloMessage);
    }

    /**
     * Send an encoded message to the websocket endpoint.
     */
    public void send(Message message)  {
      try {
        session.getAsyncRemote().sendText(MAPPER.writeValueAsString(message)).get();
        Log.info("Test client sent message: " + message);
      } catch (IOException | ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Send an arbitrary string message to the websocket endpoint.
     */
    public void send(String message)  {
      try {
        session.getAsyncRemote().sendText(message).get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Block until a message of the given type is received. Return it.
     *
     * @throws RuntimeException if the message is not received within the given time.
     */
    public Message waitForMessageOfType(MessageType messageType, long waitAtMostMillis) {
      return waitForMessageOfType(messageType, Duration.ofMillis(waitAtMostMillis));
    }

    /**
     * Block until a message of the given type is received. Return it.
     *
     * @throws RuntimeException if the message is not received within the given time.
     */
    public Message waitForMessageOfType(MessageType messageType, Duration timeout) {
      var start = Instant.now();
      var maxTime = start.plus(timeout);

      while (true) {
        Message message = messageHandler.poll(Duration.ofMillis(250));

        if (message == null || message.messageType() != messageType) {
          // waited too long?
          if (Instant.now().isAfter(maxTime)) {
            throw new RuntimeException("Timed out waiting for message of type " + messageType);
          }

          // otherwise loop back try again
          continue;
        }

        // Must be the message type we're looking for.
        assertEquals(messageType, message.messageType());
        return message;
      }
    }

    /**
     * Verify that there are no messages received before the given timeout.
     *
     * @param timeout the maximum time to wait for messages
     */
    public void assertNoMessagesAfter(Duration timeout) {
      Message message = messageHandler.poll(timeout);
      assertNull(message, "Expected no messages, but received: " + message);
    }

    /**
     * Bounded wait for the websocket to become closed.
     * @throws RuntimeException if the websocket does not close within the given time.
     */
    public void waitForClose(long waitAtMostMillis) {
      var start = Instant.now();
      var maxWait = Duration.ofMillis(waitAtMostMillis);
      var maxTime = start.plus(maxWait);

      while (session.isOpen()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // do nothing, fallthrough
        }

        if (Instant.now().isAfter(maxTime)) {
          throw new RuntimeException("Timed out waiting for websocket to close");
        }
      }
    }

    /**
     * Close the websocket
     */
    public void closeWebsocket() {
      try {
        session.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Clear any previously received messages.
     */
    public void clearReceivedMessages() {
      messageHandler.clear();
    }

    public int receivedMessageCount() {
      return messageHandler.messages.size();
    }

    public String processIdString() {
      return mockWorkspaceProcess.pid_string;
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  @Inject
  WebsocketEndpoint websocketEndpoint;

  @TestHTTPResource("/ws")
  URI uri;

  protected void expectKnownWorkspacePids(WorkspacePid... pids) {
    stream(pids)
        .forEach(pid ->
            getKnownWorkspacePidsFromBean().add(pid)
        );
  }

  @BeforeEach
  public void resetKnownWorkspacesBean() {
    try {
      Field allowNoWorkspacesField = knownWorkspacesBean.getClass().getDeclaredField("allowNoWorkspaces");
      allowNoWorkspacesField.setAccessible(true);
      allowNoWorkspacesField.set(knownWorkspacesBean, true);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    getKnownWorkspacePidsFromBean().clear();
  }

  @BeforeEach
  public void resetAccessTokenBean() {
    setAuthToken(null);
  }

  @BeforeEach
  public void resetCurrentWebsocketCount() {
    // empty out websocketEndpoint's sessions, in case any from prior test
    // linger, which may throw off behavior of the next test.
    websocketEndpoint.sessions.clear();
  }

  /**
   * Get at the knownWorkspacesBean's knownWorkspacePIDs field through cheating reflection
   * (That functionality not needed by any external business methods.)
   */
  private Set<WorkspacePid> getKnownWorkspacePidsFromBean() {
    try {
      Field knownWorkspacePIDsField = knownWorkspacesBean
          .getClass()
          .getDeclaredField("knownWorkspacePids");
      knownWorkspacePIDsField.setAccessible(true);
      return (Set<WorkspacePid>) knownWorkspacePIDsField.get(knownWorkspacesBean);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Explicitly set the access token value in the SidecarAccessTokenBean, consulted by the filter.
   *
   * @param value The value to set the access token to.
   */
  void setAuthToken(String value) {
    try {
      Field tokenField = accessTokenBean.getClass().getDeclaredField("token");
      tokenField.setAccessible(true);
      tokenField.set(accessTokenBean, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Connect a mock workspace process to the websocket endpoint.
   *
   * @param sayHello If true, the workspace will send a HELLO message after connecting.
   * @param consumeInitialWorkspaceCount If true, the workspace will wait for the initial
   *                                     WORKSPACE_COUNT_CHANGED response to the HELLO.
   *                                     (Only meaningful when sayHello is true.)
   * @return A ConnectedWorkspace instance representing the connection.
   * */
  ConnectedWorkspace connectWorkspace(boolean sayHello, boolean consumeInitialWorkspaceCount) {
    if (consumeInitialWorkspaceCount && !sayHello) {
      throw new IllegalArgumentException("consumeInitialWorkspaceCount only makes sense when sayHello is true.");
    }

    // Given a workspace process ...
    var mockWorkspaceProcess = new MockWorkspaceProcess();

    // Make it smell as if the handshake has happened.
    var expectedTokenValue = "valid-token";
    setAuthToken(expectedTokenValue);
    // Make websocket connection request carry the right header value
    TestWebsocketClientConfigurator.setAccessToken(expectedTokenValue);

    // And known in the set of workspace ids.
    expectKnownWorkspacePids(mockWorkspaceProcess.pid);

    Log.infof("Test: ConnectedWorkspace %s connecting to websocket.", mockWorkspaceProcess.pid_string);
    Session session = null;
    try {
      session = ContainerProvider.getWebSocketContainer()
                                 .connectToServer(TestWebsocketClient.class, uri);
    } catch (DeploymentException | IOException e) {
      fail("Failed to connect to websocket endpoint: " + e.getMessage());
    }

    TestWebsocketClientMessageHandler clientHandler = new TestWebsocketClientMessageHandler();
    session.addMessageHandler(clientHandler);

    var workspace = new ConnectedWorkspace(mockWorkspaceProcess, session, clientHandler);

    if (sayHello) {
      workspace.sayHello();

      if(consumeInitialWorkspaceCount) {
        // Block until we get the initial WORKSPACE_COUNT_CHANGED message, the expected
        // response to the HELLO message.
        workspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);
      }
    }

    return workspace;
  }

  void closeSafely(ConnectedWorkspace... sessions) {
    for (var session : sessions) {
      if (session != null) {
        session.closeWebsocket();
      }
    }
  }
}