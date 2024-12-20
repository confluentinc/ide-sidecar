package io.confluent.idesidecar.websocket.resources;

import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
 * Interface that provides functionality for establishing and using websocket connections.
 */
public interface WebsocketClients {

  /**
   * A websocket client message handler that stores messages received into a LinkedBlockingDeque<Message> on
   * behalf of a test. The test can then interact with LinkedBlockingDeque<Message> messages.
   */
  class TestWebsocketClientMessageHandler implements MessageHandler.Whole<String> {

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
  class TestWebsocketClientConfigurator extends ClientEndpointConfig.Configurator {

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
  class TestWebsocketClient{
  }

  /**
   * Finally, a convenience bundle of the above:
   * 1. a mock workspace process,
   * 2. its websocket session,
   * 3. and the message handler that will store messages received by the client.
   * See {@link #connectWorkspace}
   */
  record ConnectedWorkspace (
      WorkspacePid workspacePid,
      Session session,
      TestWebsocketClientMessageHandler messageHandler
  ) {

    public String processIdString() {
      return workspacePid.toString();
    }

    /**
     * Send a HELLO message to the websocket endpoint. Do not wait for a response.
     * Caller can provide an alternate pid to hello with if so desired, for either
     * the pid spelled in general message header, or in the body of the hello.
     * Defaults to the pid of the mock workspace process.
     *
     * @return this object for chaining
     */
    public ConnectedWorkspace sayHello() {
      assertNotNull(workspacePid, "No workspace pid to say hello with");
      var helloMessage = new Message(
          new MessageHeaders(
              MessageType.WORKSPACE_HELLO, workspacePid.toString(),
              "message-id-here"),
          new HelloBody(workspacePid.id())
      );
      return send(helloMessage);
    }

    /**
     * Send an encoded message to the websocket endpoint.
     * @return this object for chaining
     */
    public ConnectedWorkspace send(Message message)  {
      try {
        session.getAsyncRemote().sendText(MAPPER.writeValueAsString(message)).get();
        Log.info("Test client sent message: " + message);
      } catch (IOException | ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    /**
     * Send an arbitrary string message to the websocket endpoint.
     * @return this object for chaining
     */
    public ConnectedWorkspace send(String message)  {
      try {
        session.getAsyncRemote().sendText(message).get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
      return this;
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
     * @return this object for chaining
     */
    public ConnectedWorkspace assertNoMessagesAfter(Duration timeout) {
      Message message = messageHandler.poll(timeout);
      assertNull(message, "Expected no messages, but received: " + message);
      return this;
    }

    /**
     * Bounded wait for the websocket to become closed.
     * @return this object for chaining
     * @throws RuntimeException if the websocket does not close within the given time.
     */
    public ConnectedWorkspace waitForClose(long waitAtMostMillis) {
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
      return this;
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
  }

  ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Establish a websocket client session, without
   * {@link ConnectedWorkspace#sayHello() binding to a workspace process}.
   *
   * @param uri              the URI of the websocket endpoint
   * @param authToken        the auth token to use for the connection
   * @param workspaceProcess the ID of the workspace client process that is connecting
   * @return the websocket client session
   */
  default ConnectedWorkspace connectWorkspace(
      URI uri,
      String authToken,
      WorkspacePid workspaceProcess
  ) {
    return connectWorkspace(uri, authToken, workspaceProcess, false, null);
  }

  /**
   * Connect a mock workspace process to the websocket endpoint.
   *
   * @param uri                            the URI of the websocket endpoint
   * @param authToken                      the auth token to use for the connection
   * @param workspaceProcess               the ID of the workspace client process that is connecting
   * @param sayHello                       true if the workspace should send a HELLO message after
   *                                       connecting
   * @param maxTimeToWaitForWorkspaceCount the maximum time to wait for the initial
   *                                       WORKSPACE_COUNT_CHANGED event after the HELLO;
   *                                       may be null if not waiting for this event
   * @return A ConnectedWorkspace instance representing the connection.
   */
  default ConnectedWorkspace connectWorkspace(
      URI uri,
      String authToken,
      WorkspacePid workspaceProcess,
      boolean sayHello,
      Duration maxTimeToWaitForWorkspaceCount
  ) {
    if (maxTimeToWaitForWorkspaceCount != null && !sayHello) {
      throw new IllegalArgumentException("maxTimeToWaitForWorkspaceCount only makes sense when saying hello.");
    }

    Log.infof("Connecting websocket at %s", uri);
    Session session = null;
    try {
      session = ContainerProvider.getWebSocketContainer()
                                 .connectToServer(TestWebsocketClient.class, uri);
    } catch (DeploymentException | IOException e) {
      fail("Failed to connect to websocket endpoint: " + e.getMessage());
    }

    TestWebsocketClientConfigurator.setAccessToken(authToken);
    TestWebsocketClientMessageHandler clientHandler = new TestWebsocketClientMessageHandler();
    session.addMessageHandler(clientHandler);

    var workspace = new ConnectedWorkspace(workspaceProcess, session, clientHandler);

    if (sayHello) {
      workspace = workspace.sayHello();

      if(maxTimeToWaitForWorkspaceCount != null) {
        // Block until we get the initial WORKSPACE_COUNT_CHANGED message, the expected
        // response to the HELLO message.
        workspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, maxTimeToWaitForWorkspaceCount);
      }
    }

    return workspace;
  }

  default void closeSafely(ConnectedWorkspace... sessions) {
    for (var session : sessions) {
      if (session != null) {
        session.closeWebsocket();
      }
    }
  }

  /**
   * Get at the knownWorkspacesBean's knownWorkspacePIDs field through cheating reflection
   * (That functionality not needed by any external business methods.)
   * @param knownWorkspacesBean the KnownWorkspacesBean bean to get the field from
   */
  static Set<WorkspacePid> getKnownWorkspacePidsFromBean(KnownWorkspacesBean knownWorkspacesBean) {
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
   * Utility that will add the specified workspace process IDs to the {@link KnownWorkspacesBean}'s
   * knownWorkspacePIDs field.
   *
   * @param knownWorkspacesBean the KnownWorkspacesBean bean to which the process IDs will be added
   * @param pids                the workspace process IDs to add
   * @see #resetKnownWorkspacesBean(KnownWorkspacesBean)
   */
  static void expectKnownWorkspacePids(KnownWorkspacesBean knownWorkspacesBean, WorkspacePid... pids) {
    stream(pids)
        .forEach(pid ->
            getKnownWorkspacePidsFromBean(knownWorkspacesBean).add(pid)
        );
  }

  /**
   * Reset the knownWorkspacesBean to its initial state.
   *
   * @param knownWorkspacesBean the KnownWorkspacesBean bean
   */
  static void resetKnownWorkspacesBean(KnownWorkspacesBean knownWorkspacesBean) {
    try {
      Field allowNoWorkspacesField = knownWorkspacesBean.getClass().getDeclaredField("allowNoWorkspaces");
      allowNoWorkspacesField.setAccessible(true);
      allowNoWorkspacesField.set(knownWorkspacesBean, true);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    getKnownWorkspacePidsFromBean(knownWorkspacesBean).clear();
  }

  /**
   * Explicitly set the access token value in the SidecarAccessTokenBean, consulted by the filter.
   *
   * @param accessTokenBean the bean to set the token in
   * @param value           The value to set the access token to.
   * @see #resetAccessTokenBean(SidecarAccessTokenBean) 
   */
  static void setAuthToken(SidecarAccessTokenBean accessTokenBean, String value) {
    try {
      Field tokenField = accessTokenBean.getClass().getDeclaredField("token");
      tokenField.setAccessible(true);
      tokenField.set(accessTokenBean, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Utility to reset the access token in the SidecarAccessTokenBean.
   * @param accessTokenBean the bean to reset
   * @see #setAuthToken(SidecarAccessTokenBean, String) 
   */
  static void resetAccessTokenBean(SidecarAccessTokenBean accessTokenBean) {
    setAuthToken(accessTokenBean, null);
  }
}