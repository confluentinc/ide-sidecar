package io.confluent.idesidecar.websocket.resources;

import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.filters.WorkspaceProcessIdFilter;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.confluent.idesidecar.websocket.messages.DynamicMessageBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/**
 * Tests over workspaces connecting to /ws as websocket clients, access filtering, and message handling.
 */
@QuarkusTest
public class WebsocketEndpointTest {

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @Inject
  KnownWorkspacesBean knownWorkspacesBean;


  @TestHTTPResource("/ws")
  URI uri;

  // First some static inner classes to handle websocket client side for this test.

  /**
   * A websocket client message handler that stores messages received into a LinkedBlockingDeque<Message> on
   * behalf of a test. The test can then interact with LinkedBlockingDeque<Message> messages.
   * */
  public static class TestWebsocketClientMessageHandler implements MessageHandler.Whole<String>
  {
    // Where this client will store deserialized messages received.
    public final LinkedBlockingDeque<Message> messages = new LinkedBlockingDeque<>();

    // likewise, but for the raw message json strings. Needed for tests which send explictly
    // unknown message types whose serialization will coerce to DynamicMessageBody + MessageTypes.UNKNOWN,
    // but when delivered through to other workspaces, the original message type should be preserved.
    public final LinkedBlockingDeque<String> rawMessageStrings = new LinkedBlockingDeque<>();

    // For deserializing messages.
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void onMessage(String messageString)
    {
      rawMessageStrings.add(messageString);
      try {
        // Deserialize json -> Message, add to our list of messages so that the main
        // test can check them.
        Message message = mapper.readValue(messageString, Message.class);
        messages.add(message);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /** Empty out any recieved messages */
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
   * */
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
  private record ConnectedWorkspace (
      MockWorkspaceProcess mockWorkspaceProcess,
      Session session,
      TestWebsocketClientMessageHandler messageHandler
  ) {

    /**
     * Send an encoded message to the websocket endpoint.
     */
    public void send(Message message) throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      session.getBasicRemote().sendText(mapper.writeValueAsString(message));
    }

    /**
     * Send an arbitrary string message to the websocket endpoint.
     */
    public void send(String message) throws IOException {
      session.getBasicRemote().sendText(message);
    }
  }


  /**
   * Test websocket lifecycle using two mock workspace processes.
   * 1. Handshake with the first workspace process to establish the access token.
   * 2. Have each workspace the health check endpoint with the access token and workspace process id headers, getting
   *    the workspace pids into the known set of workspaces (via WorkspaceProcessIdFilter -> KnownWorkspacesBean).
   * 3. Have each workspace connect to the websocket endpoint (providing both the access token as a request
   *      header and its workspace pid in the query string, and verify that sidecar immediately sends a message
   *      to the client with the current workspace count. Expect == 1 for the first workspace, == 2 for the second.
   * 4. The first workspace should receive a message about the second workspace connecting. Verify it.
   * 5. Have the first workspace send an unknown message type message. It should be proxied through to the second
   *   workspace verbatim.
   * 6. Close the second workspace's websocket connection. The first workspace should receive a message about the
   *   second workspace disconnecting. Verify it.
   *
   * Providing the query string at connection time along with the access token in the headers was
   * awkward to figure out here in java-land. I had to split the implementation of the websocket client
   * into three different classes -- one that injects the headers at connect time, one that receives
   * messages in such a way that the test can get at each individual client's received messages, and
   * then finally the actual client class that connects to the websocket endpoint. That middle
   * class was necessary because I couldn't find a way to control the construction of the client
   * class. Your mileage may vary.
   *
   * This test drives the MockWorkspaceProcess, TestWebsocketClientMessageHandler, etc. explicitly
   * for all the gory details. Other tests that need start with a happy connected workspace
   * websocket will use {@link #connectWorkspace} to handle all the setup.
   */
  @Test
  public void testWebsocketLifecycle() throws DeploymentException, IOException, InterruptedException {
    // Given two workspaces ...
    MockWorkspaceProcess[] mockWorkspaceProcesses = new MockWorkspaceProcess[]{
        new MockWorkspaceProcess(),
        new MockWorkspaceProcess(),
    };

    // 1. Handshake as the first process to get the access token, just as expected
    // in real life.
    String expectedTokenValue =
        RestAssured.given()
            .when()
            .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
                mockWorkspaceProcesses[0].pid_string)
            .get("/gateway/v1/handshake")
            .then()
            .assertThat()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getString("auth_secret");


    // Set the access token value in TestWebsocketClientConfigurator
    TestWebsocketClientConfigurator.setAccessToken(expectedTokenValue);


    // 2. Have both workspaces hit the health check endpoint with the auth token and
    // workspace process id headers. This gets their pids in the known set of workspaces.
    for (MockWorkspaceProcess mockWorkspaceProcess : mockWorkspaceProcesses) {
      RestAssured.given()
          .when()
          .header("Authorization", "Bearer " + expectedTokenValue)
          .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
              mockWorkspaceProcess.pid_string)
          .get("/gateway/v1/health/ready")
          .then()
          .assertThat()
          .statusCode(200);
    }

    // Both workspace pids should now be in the known set of workspaces.
    var knownWorkspacePids = getKnownWorkspacePidsFromBean();
    for (MockWorkspaceProcess mockWorkspaceProcess : mockWorkspaceProcesses) {
      Assertions.assertTrue(
          knownWorkspacePids.contains(Long.valueOf(mockWorkspaceProcess.pid_string)));
    }

    // Now, let's masquerade each workspace connecting websocket clients to the endpoint.
    var websocketSessions = new ArrayList<Session>();
    var messageHandlers = new ArrayList<TestWebsocketClientMessageHandler>();
    int expectedWorkspaceCount = 0;

    // 3. Have each mock workspace processes connect to the websocket endpoint.
    for (MockWorkspaceProcess mockWorkspaceProcess : mockWorkspaceProcesses) {
      // adjust uri to have query string with workspace process id
      URI uri = URI.create(
          this.uri.toString() + "?workspace_id=" + mockWorkspaceProcess.pid_string);

      Session session = ContainerProvider.getWebSocketContainer()
          .connectToServer(TestWebsocketClient.class, uri);

      TestWebsocketClientMessageHandler clientHandler = new TestWebsocketClientMessageHandler();
      session.addMessageHandler(clientHandler);

      websocketSessions.add(session);
      messageHandlers.add(clientHandler);

      expectedWorkspaceCount++;

      // block for at most 1s until the websocket client has received the first message,
      // sent by sidecar to the client upon connection, telling if of the number of
      // workspaces currently connected (inclusive).
      var message = clientHandler.messages.poll(1, TimeUnit.SECONDS);
      if (message == null) {
        throw new RuntimeException("Timed out waiting for client to receive message");
      }

      // The message should be a WORKSPACE_COUNT_CHANGED message describing the expected
      // number of workspaces connected.
      Assertions.assertEquals(MessageType.WORKSPACE_COUNT_CHANGED, message.messageType());
      Assertions.assertEquals(((WorkspacesChangedBody) message.body()).workspaceCount(),
          expectedWorkspaceCount);
    }

    // let things settle ...
    Thread.sleep(100);

    // 4. The first workspace should have received one additional messages, when the second workspace connected.
    var secondAnnouncement = messageHandlers.getFirst().messages.poll(1, TimeUnit.SECONDS);
    if (secondAnnouncement == null) {
      throw new RuntimeException("Timed out waiting for client to receive message");
    }
    Assertions.assertEquals(MessageType.WORKSPACE_COUNT_CHANGED, secondAnnouncement.messageType());
    Assertions.assertEquals(2,
        ((WorkspacesChangedBody) secondAnnouncement.body()).workspaceCount());

    // 5. Now let's have one workspace send an arbitrary, unknown message type to sidecar.
    // It should be proxied through *unchanged*, including the random message type, to the other workspace.

    // Need to build by string because we explicitly want to use a message type that isn't
    // known to the sidecar.
    String bogusMessageTypeMessage = """
        {
          "headers": {
            "message_type": "random_workspace_message",
            "originator": "%s",
            "message_id": "message-id-here"
          },
          "body": {
            "foonly": 3
          }
        }
        """.formatted(mockWorkspaceProcesses[0].pid_string);

    var secondWorkspaceMessageHandler = messageHandlers.get(1);
    secondWorkspaceMessageHandler.clear(); // clear out any messages received so far

    // send the message from first workspace.
    websocketSessions.getFirst().getBasicRemote().sendText(bogusMessageTypeMessage);

    // The second workspace should receive the message, and it should be unchanged, esp.
    // the message tyoe.
    var randomMessage = messageHandlers.get(1).messages.poll(1, TimeUnit.SECONDS);
    if (randomMessage == null) {
      throw new RuntimeException("Timed out waiting for client to receive message");
    }

    Assertions.assertEquals("message-id-here", randomMessage.headers().id());
    Assertions.assertEquals(randomMessage.headers().originator(),
        mockWorkspaceProcesses[0].pid_string);
    Assertions.assertEquals(3,
        ((DynamicMessageBody) randomMessage.body()).getProperties().get("foonly"));

    // to test the original message type, though, must look at the received raw message string,
    // as the deserialized message type will have been coerced to MessageType.UNKNOWN.
    var rawMessageString = secondWorkspaceMessageHandler.rawMessageStrings.getFirst();
    Assertions.assertTrue(
        rawMessageString.contains("\"message_type\": \"random_workspace_message\""));

    // 6. Close the second workspace session. The first should receive a message about it having disconnected.
    websocketSessions.get(1).close();

    var message = messageHandlers.getFirst().messages.poll(1, TimeUnit.SECONDS);
    if (message == null) {
      throw new RuntimeException("Timed out waiting for client to receive message");
    }
    // Should describe just one workspace connected.
    Assertions.assertEquals(MessageType.WORKSPACE_COUNT_CHANGED, message.messageType());
    Assertions.assertEquals(1, ((WorkspacesChangedBody) message.body()).workspaceCount());
  }

  @Test
  public void testConnectWithoutAccessHeaderFails() {
    // Given a workspace process ...
    MockWorkspaceProcess mockWorkspaceProcess = new MockWorkspaceProcess();

    // known in the set of workspace ids.
    getKnownWorkspacePidsFromBean().add(Long.valueOf(mockWorkspaceProcess.pid_string));

    // When the workspace process tries to connect to the websocket endpoint without the access token header ...
    RestAssured.given()
        .when()
        .get("/ws?workspace_id=" + mockWorkspaceProcess.pid_string)
        .then()
        .assertThat()
        .statusCode(401);
  }

  @Test
  public void testConnectWithInvalidAccessHeaderFails() {
    // Given a workspace process ...
    MockWorkspaceProcess mockWorkspaceProcess = new MockWorkspaceProcess();

    // Make it smell as if the handshake has happened.
    setAuthToken("valid-token");

    getKnownWorkspacePidsFromBean().add(Long.valueOf(mockWorkspaceProcess.pid_string));

    // When the workspace process tries to connect to the websocket endpoint with an invalid access token header,
    // but a known workspace id, it should fail ...
    RestAssured.given()
        .when()
        .header("Authorization", "Bearer invalid-token")
        .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
            mockWorkspaceProcess.pid_string)
        .get("/ws?workspace_id=" + mockWorkspaceProcess.pid_string)
        .then()
        .assertThat()
        .statusCode(401);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/ws", // No workspace_id parameter at all
      "/ws?workspace_id=sdfsdfsdfsd", // not a number
      "/ws?workspace_id=1234" // an unknown workspace id
  })
  public void testConnectWithBadWorkspacePidParameterFails(String url) {
    // Given a workspace process ...
    MockWorkspaceProcess mockWorkspaceProcess = new MockWorkspaceProcess();

    // Set auth token.
    setAuthToken("valid-token");

    // Hit the WS route with query string workspace id mismatch, but have it smell
    // like a websocket connection request to make it into WebsocketEndpoint.onOpen().

    // The opOpen() method will check the workspace id in the query string against the known set of
    // workspace pids, and if it doesn't match, it will immediately close the connection w/o
    // a response. Can see "Unauthorized workspace id: 1234. Closing session." in the logs.

    boolean thrown = false;
    try {

      RestAssured.given()
          .when()
          .header("Authorization", "Bearer valid-token")
          .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
              mockWorkspaceProcess.pid_string)
          .header("Connection", "Upgrade")
          .header("Upgrade", "websocket")
          .header("Sec-WebSocket-Version", 13)
          .header("Sec-WebSocket-Key", "key")
          .get(url); // hitting the /ws route, but bad or missing workspace id query string param

    } catch (Exception e) {
      // Then the connection should be closed immediately w/o a response. This angers RestAssured,
      // but is fine for our purposes.
      thrown = true;
    }

    Assertions.assertTrue(thrown);
  }

  /** Test bad deserialize handling within WebsocketEndpoint::onMessage(), when parseAndValidateMessage() raises. */
  @Test
  public void testSendingInvalidMessageStructureClosesSession() throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    ConnectedWorkspace connectedWorkspace = connectWorkspace();

    // When the workspace sends a message with an invalid JSON structure ...
    connectedWorkspace.send("not a json object");

    // Then the session should be closed very soon after.
    Thread.sleep(1000);

    // Should be closed now.
    Assertions.assertFalse(connectedWorkspace.session.isOpen());
  }

  /** Test some error cases within WebsocketEndpoint::onMessage() when the originator header value
   * is wrong given the sending workspace session. */
  @ValueSource(strings = {
      "not-a-valid-pid",
      "sidecar",
      "1234" // an unknown workspace id
  })
  @ParameterizedTest
  public void testWrongOriginatorValueInMessageClosesSession(String invalidOriginator) throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    ConnectedWorkspace connectedWorkspace = connectWorkspace();

    // When the workspace sends a message with an invalid originator value not corresponding with
    // their process id ...
    Message message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, invalidOriginator, "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // Then the session should be closed very soon after.
    Thread.sleep(1000);

    // Should be closed now.
    Assertions.assertFalse(connectedWorkspace.session.isOpen());
  }

  /** Connect a mock workspace process to the websocket endpoint successfully. */
  private ConnectedWorkspace connectWorkspace() {
    // Given a workspace process ...
    MockWorkspaceProcess mockWorkspaceProcess = new MockWorkspaceProcess();

    // Make it smell as if the handshake has happened.
    String expectedTokenValue = "valid-token";
    setAuthToken(expectedTokenValue);
    // Make websocket connection request carry the right header value
    TestWebsocketClientConfigurator.setAccessToken(expectedTokenValue);

    // And known in the set of workspace ids.
    getKnownWorkspacePidsFromBean().add(Long.valueOf(mockWorkspaceProcess.pid_string));

    // When the workspace process tries to connect to the websocket endpoint with the access token header ...
    URI uri = URI.create(
        this.uri.toString() + "?workspace_id=" + mockWorkspaceProcess.pid_string);

    Session session = null;
    try {
      session = ContainerProvider.getWebSocketContainer()
          .connectToServer(TestWebsocketClient.class, uri);
    } catch (DeploymentException | IOException e) {
      throw new RuntimeException(e);
    }

    TestWebsocketClientMessageHandler clientHandler = new TestWebsocketClientMessageHandler();
    session.addMessageHandler(clientHandler);

    return new ConnectedWorkspace(mockWorkspaceProcess, session, clientHandler);
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

  /**
   * Get at the knownWorkspacesBean's knownWorkspacePIDs field through cheating reflection
   * (That functionality not needed by any external business methods.)
   */
  private Set<Long> getKnownWorkspacePidsFromBean() {
    try {
      Field knownWorkspacePIDsField = knownWorkspacesBean.getClass()
          .getDeclaredField("knownWorkspacePIDs");
      knownWorkspacePIDsField.setAccessible(true);
      return (Set<Long>) knownWorkspacePIDsField.get(knownWorkspacesBean);
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

}

