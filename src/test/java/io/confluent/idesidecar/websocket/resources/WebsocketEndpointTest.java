package io.confluent.idesidecar.websocket.resources;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.filters.WorkspaceProcessIdFilter;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.confluent.idesidecar.websocket.messages.DynamicMessageBody;
import io.confluent.idesidecar.websocket.messages.HelloBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ProtocolErrorBody;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.logging.Log;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
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

  @Inject
  WebsocketEndpoint websocketEndpoint;


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
    public void onMessage(String messageString) {
      Log.info("Test client received message: " + messageString);
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

    /** Empty out any received messages */
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
    public void sayHello(WorkspacePid headerPid,  WorkspacePid bodyPid) {

      headerPid = headerPid != null ? headerPid : mockWorkspaceProcess.pid;
      bodyPid = bodyPid != null ? bodyPid : mockWorkspaceProcess.pid;

      Message helloMessage = new Message(
          new MessageHeaders(MessageType.WORKSPACE_HELLO, headerPid.toString(),
              "message-id-here"),
          new HelloBody(bodyPid.id())
      );
      send(helloMessage);
    }

    /**
     * Send an encoded message to the websocket endpoint.
     */
    public void send(Message message)  {
      ObjectMapper mapper = new ObjectMapper();
      try {
        session.getAsyncRemote().sendText(mapper.writeValueAsString(message)).get();
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
      }  catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Block until a message of the given type is received. Return it.
     *
     * @throws RuntimeException if the message is not received within the given time.
     */
    public Message waitForMessageOfType(MessageType messageType, long waitAtMostMillis) {
      long start = System.currentTimeMillis();

      while (true) {
        Message message = null;

        try {
          message = messageHandler.messages.poll(250, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // do nothing, fallthrough
        }

        if (message == null || message.messageType() != messageType) {
          // waited too long?
          if (System.currentTimeMillis() - start > waitAtMostMillis) {
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
     * Bounded wait for the websocket to become closed.
     * @throws RuntimeException if the websocket does not close within the given time.
     */
    public void waitForClose(long waitAtMostMillis) {
      long start = System.currentTimeMillis();

      while (session.isOpen()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // do nothing, fallthrough
        }

        if (System.currentTimeMillis() - start > waitAtMostMillis) {
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


  /**
   * Test websocket lifecycle using two mock workspace processes.
   * 1. Have each make websocket connections to the endpoint and send a hello message.
   * 2. Have each expect a WORKSPACE_COUNT_CHANGED message with the expected count (1, 2).
   * 3. Have the first one expect a subsequent WORKSPACE_COUNT_CHANGED message with the expected new
   *    count (2)
   * 4. Have first one send a message that should be broadcast to all workspaces.
   * 5. Have the second one expect to receive that message.
   * 6. Have the first one disconnect.
   * 7. Have the second one expect a WORKSPACE_COUNT_CHANGED message with the expected new count (1).
   * 8. Have the second one send a message that should be broadcast to all workspaces, will not be
   *    received by anyone.
   * 9. Have the second one disconnect.
   */
  @Test
  public void testWebsocketLifecycle()
  {
    var firstWorkspace = connectWorkspace(true);

    // Block until we get the initial WORKSPACE_COUNT_CHANGED message.
    var message = firstWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);
    var workspacesChangedBody = (WorkspacesChangedBody) message.body();
    assertEquals(1, workspacesChangedBody.workspaceCount());

    // Now connect a second workspace.
    var secondWorkspace = connectWorkspace(true);

    var workspaces = List.of(firstWorkspace, secondWorkspace);
    // Both should get WORKSPACE_COUNT_CHANGED message with count == 2
    for (var workspace : workspaces) {
      message = workspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);
      workspacesChangedBody = (WorkspacesChangedBody) message.body();
      assertEquals(2, workspacesChangedBody.workspaceCount());
    }

    // Have the first workspace send a message that should be broadcast to all workspaces.
    var broadcastMessage = new Message(
        new MessageHeaders(MessageType.UNKNOWN, firstWorkspace.processIdString(), "broadcast-message-id"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    firstWorkspace.send(broadcastMessage);

    // The second workspace should receive that message.
    message = secondWorkspace.waitForMessageOfType(MessageType.UNKNOWN, 1000);
    assertEquals("broadcast-message-id", message.id());

    // Now have first workspace disconnect.
    firstWorkspace.closeWebsocket();

    // Second workspace should get WORKSPACE_COUNT_CHANGED message with count == 1
    message = secondWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 2000);
    workspacesChangedBody = (WorkspacesChangedBody) message.body();
    assertEquals(1, workspacesChangedBody.workspaceCount());

    // Have the second workspace send a message that should be broadcast to all workspaces, but
    // no one is there to receive it.
    broadcastMessage = new Message(
        new MessageHeaders(MessageType.UNKNOWN, secondWorkspace.processIdString(), "broadcast-message-id-2"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    secondWorkspace.send(broadcastMessage);

    // Should not receive any messages.

    // assert raises RuntimeException since no message received in time ...
    assertThrows(RuntimeException.class, () -> {
      secondWorkspace.waitForMessageOfType(MessageType.UNKNOWN, 1000);
    });

    // Now have second workspace disconnect.
    secondWorkspace.closeWebsocket();
  }

  @Test
  public void testConnectWithoutAccessHeaderFails() {
    // Given a workspace process ...
    var mockWorkspaceProcess = new MockWorkspaceProcess();

    // known in the set of workspace ids.
    getKnownWorkspacePidsFromBean().add(mockWorkspaceProcess.pid);

    // When the workspace process tries to connect to the websocket endpoint without the access token header ...
    given()
        .when()
        .get("/ws")
        .then()
        .assertThat()
        .statusCode(401);
  }

  @Test
  public void testConnectWithInvalidAccessHeaderFails() {
    // Given a workspace process ...
    var mockWorkspaceProcess = new MockWorkspaceProcess();

    // Make it smell as if the handshake has happened.
    setAuthToken("valid-token");

    getKnownWorkspacePidsFromBean().add(mockWorkspaceProcess.pid);;

    // When the workspace process tries to connect to the websocket endpoint with an invalid access token header,
    // but a known workspace id, it should fail ...
    given()
        .when()
        .header("Authorization", "Bearer invalid-token")
        .header(
            WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
            mockWorkspaceProcess.pid_string
        )
        .get("/ws")
        .then()
        .assertThat()
        .statusCode(401);
  }


  /** Test bad deserialize handling within WebsocketEndpoint::onMessage() -> handleHelloMessage() -> deserializeMessage()
   * and deserializeMessage() cannot deserialize non-json. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testSendingInvalidMessageStructureClosesSession(boolean sayHelloFirst) throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(sayHelloFirst);

    // When the workspace sends first a message with an invalid JSON structure ...
    connectedWorkspace.send("not a json object");

    // then should receive an error message
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorString = ((ProtocolErrorBody) errorMessage.body()).error();
    assertTrue(errorString.startsWith("Unparseable message"));

    // Then the session should be closed very soon after.
    connectedWorkspace.waitForClose(10000);
  }

  /** Test sending a random message as the first message instead of a WORKSPACE_HELLO message. */
  @Test
  public void testSendingRandomMessageFirstClosesSession() throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false);

    // When the workspace sends a random message as the first message ...
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // then should receive an error message
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorString = ((ProtocolErrorBody) errorMessage.body()).error();
    assertEquals("Expected WORKSPACE_HELLO message, got UNKNOWN. Closing session.",
        errorString);

    // Then the session should be closed very soon after.
    connectedWorkspace.waitForClose(1000);
  }

  /** Test sending a WORKSPACE_HELLO message with an originator+payload pid mismatch. */
  @Test
  public void testSendingHelloMessageWithMismatchedOriginator() throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false);

    // When the workspace sends a WORKSPACE_HELLO message with wrong originator value. It should
    // match the body's payload (and be known to the sidecar).
    var message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_HELLO, "1234", "message-id-here"),
        new HelloBody(connectedWorkspace.mockWorkspaceProcess.pid.id())
    );
    connectedWorkspace.send(message);

    // then should receive an error message
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorString = ((ProtocolErrorBody) errorMessage.body()).error();

    var expectedPrefix = String.format("Workspace %s sent message with incorrect originator value: 1234. Removing and closing session", connectedWorkspace.processId());
    assertTrue(
        errorString.startsWith(expectedPrefix)
    );

    // Then the session should be closed very soon after.
    connectedWorkspace.waitForClose(1000);
  }


  /** Test sending a WORKSPACE_HELLO message with an originator+payload pid mismatch. */
  @Test
  public void testSendingHelloMessageWithUnknownProcessId() throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false);

    // When the workspace sends a WORKSPACE_HELLO message with both header and body pid values
    // not corresponding with their process id as previously presented...
    var message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_HELLO, "1234", "message-id-here"),
        new HelloBody(1234)
    );
    connectedWorkspace.send(message);

    // then should receive an error message
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorString = ((ProtocolErrorBody) errorMessage.body()).error();

    assertEquals(
        "handleHelloMessage: Unknown workspace pid 1234. Closing session.",
        errorString
    );

    // Then the session should be closed very soon after.
    connectedWorkspace.waitForClose(1000);
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
    var connectedWorkspace = connectWorkspace(true);

    // When the workspace sends a message with an invalid originator value not corresponding with
    // their process id ...
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, invalidOriginator, "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // Should get a PROTOCOL_ERROR message back from the sidecar complaining about 'originator' and the session should be closed.
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorBody = (ProtocolErrorBody) errorMessage.body();
    assert errorBody.error().contains("originator");
    assert errorBody.originalMessageId().equals("message-id-here");

    // Should then be closed server-side.
    connectedWorkspace.waitForClose(1000);
  }

  @Test
  public void testOnMessageHandlingUnknownSession() throws IOException{

    Log.infof("Test: testOnMessageHandlingUnknownSession, websocketEndpoint: %s", websocketEndpoint);
    Log.infof("Test: testOnMessageHandlingUnknownSession, websocketEndpoint sessions: %s", System.identityHashCode(websocketEndpoint.sessions));

    // Test the case where a message is received from a session that is not known to the sidecar.
    // (which should be impossible, but we should handle it gracefully.)
    ConnectedWorkspace connectedWorkspace = connectWorkspace(true);

    // wait for the workspace count change message.
    connectedWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);

    // should be one session kept track of in the sessions map, but is almost like
    // the injection is working right / we're getting a different instance or something.
    assertEquals(1, websocketEndpoint.sessions.size());

    // Now reach in and remove this session from the known sessions map, as if
    // some other code path had made a mistake.
    websocketEndpoint.sessions.clear();

    // Now send a message from the workspace, should hit the first error block
    // in onMessage();
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    connectedWorkspace.send(message);

    // should get a PROTOCOL_ERROR message back from the sidecar complaining about 'unknown session'
    // and the session should be closed.
    var errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 5000);
    var errorBody = (ProtocolErrorBody) errorMessage.body();
    assert errorBody.error().contains("Received message from unknown session");

    // Should then be closed server-side.
    connectedWorkspace.waitForClose(1000);
  }

  @Test
  public void testWorkspaceBroadcastingWhenNoOtherWorkspaces() throws IOException, InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(true);

    // Block until we get the initial WORKSPACE_COUNT_CHANGED message.
    connectedWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);

    // clear any messages received so far.
    connectedWorkspace.clearReceivedMessages();

    // When the workspace sends a message that should be broadcast to all other workspaces ...
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // then all is well. The session should still be open and the message we sent
    // should not have been received by the workspace itself.
    Thread.sleep(1000);

    assertTrue(connectedWorkspace.session.isOpen());
    assertEquals(0, connectedWorkspace.receivedMessageCount());

    // close the session so we don't leave it hanging around polluting other tests.
    connectedWorkspace.closeWebsocket();
  }

  @Test
  public void testValidateHeadersForSidecarBroadcast() {
    // originator "sidecar" is allowed and expected.
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, "sidecar", "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    WebsocketEndpoint.validateHeadersForSidecarBroadcast(message);

    // originator "1234" indicating is from a workspace is not allowed, however.
    var message2 = new Message(
        new MessageHeaders(MessageType.UNKNOWN, "1234", "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    assertThrows(IllegalArgumentException.class, () ->
      WebsocketEndpoint.validateHeadersForSidecarBroadcast(message2)
    );
  }

  @Test
  public void testSendingMismatchedBodyType() throws IOException {
    // Given a workspace connected happy websocket ...
    ConnectedWorkspace connectedWorkspace = connectWorkspace(false);

    // Send wrong body type for the HELLO message.
    Message message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_HELLO, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // then should receive an error message
    Message errorMessage = connectedWorkspace.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorString = ((ProtocolErrorBody) errorMessage.body()).error();
    assertEquals(
        "Expected HelloBody message body, got DynamicMessageBody. Closing session.",
        errorString
    );

    // Then the session should be closed very soon after.
    connectedWorkspace.waitForClose(1000);
  }

  @Test
  public void testDisconnectingBeforeErrorSent() throws IOException, InterruptedException {
    // Send a bad hello message, but disconnect before the error message is sent.
    // Gets coverage over sendErrorAndCloseSession() error paths.
    ConnectedWorkspace connectedWorkspace = connectWorkspace(false);
    var badHelloMessage = new Message(
        new MessageHeaders(MessageType.WORKSPACE_HELLO, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );

    connectedWorkspace.send(badHelloMessage);
    // hopefully close the session before the reply error message is sent.
    connectedWorkspace.closeWebsocket();

    // pause a bit to let the sidecar try to send the error message.
    Thread.sleep(2000);

    // Sessions should be empty.
    assertEquals(0, websocketEndpoint.sessions.size());
  }

  @Test
  void testTwoConnectionsFromSameWorkspacePid() {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(true);
    var firstWorkspacePid = connectedWorkspace.processId();
    // Block until we get the initial WORKSPACE_COUNT_CHANGED message.
    connectedWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);

    // Now make second websocket connection, saying hello with the same pid as from the first
    // (in the body of the message, not the header. We want to get to a later error than if
    // the wrong pid was in the header.)
    var connectedWorkspace2 = connectWorkspace(false);
    connectedWorkspace2.sayHello(firstWorkspacePid, firstWorkspacePid);

    // The second workspace should get a PROTOCOL_ERROR message back from the sidecar complaining about
    // 'duplicate workspace pid' and the session should be closed.
    var errorMessage = connectedWorkspace2.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);
    var errorBody = (ProtocolErrorBody) errorMessage.body();
    var expected = "Workspace id %s already connected. Closing session.".formatted(firstWorkspacePid);
    assertEquals(expected, errorBody.error());

    // Should then be closed server-side.
    connectedWorkspace2.waitForClose(1000);

    // The first workspace should still be connected.
    assertTrue(connectedWorkspace.session.isOpen());

    // close the session so we don't leave it hanging around polluting other tests.
    connectedWorkspace.closeWebsocket();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testOnError(boolean shouldFindSession) throws IOException {
    // Connect as a workspace to establish a session, then directly call onError() with the
    // server side session handle.
    var connectedWorkspace = connectWorkspace(true);

    // Block until we get the initial WORKSPACE_COUNT_CHANGED message.
    connectedWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);

    // should have exactly one session in the sessions map.
    assertEquals(1, websocketEndpoint.sessions.size());

    // Grab that session from the sessions map.
    var session = websocketEndpoint.sessions.values().iterator().next().session();

    // If we're testing the case where the session is not found in the sessions map, remove it.
    if (!shouldFindSession) {
      websocketEndpoint.sessions.clear();
    }

    // Call onError() with the session.
    websocketEndpoint.onError(session, new RuntimeException("test error"));

    // The client-side session should be closed.
    connectedWorkspace.waitForClose(1000);

    // and the entry removed from the sessions map (if it was there to begin with).
    if (shouldFindSession) {
      assertEquals(0, websocketEndpoint.sessions.size());
    }
  }

  /** Connect a mock workspace process to the websocket endpoint successfully. */
  private ConnectedWorkspace connectWorkspace(boolean sayHello) {
    // Given a workspace process ...
    var mockWorkspaceProcess = new MockWorkspaceProcess();

    // Make it smell as if the handshake has happened.
    var expectedTokenValue = "valid-token";
    setAuthToken(expectedTokenValue);
    // Make websocket connection request carry the right header value
    TestWebsocketClientConfigurator.setAccessToken(expectedTokenValue);

    // And known in the set of workspace ids.
    getKnownWorkspacePidsFromBean().add(mockWorkspaceProcess.pid);

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
    }

    return workspace;
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
      Field knownWorkspacePIDsField = knownWorkspacesBean.getClass()
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

}
