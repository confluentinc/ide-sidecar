package io.confluent.idesidecar.websocket.resources;

import static io.restassured.RestAssured.given;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.filters.WorkspaceProcessIdFilter;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.confluent.idesidecar.websocket.messages.DynamicMessageBody;
import io.confluent.idesidecar.websocket.messages.HelloBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ProtocolErrorBody;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.confluent.idesidecar.websocket.resources.WebsocketEndpoint.WorkspaceWebsocketSession;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests over workspaces connecting to /ws as websocket clients, access filtering, and message handling.
 */
@QuarkusTest
public class WebsocketEndpointTest extends AbstractWebsocketTestBase {

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
  public void testWebsocketLifecycle() {
    var firstWorkspace = connectWorkspace(true, false);

    // Block until we get the initial WORKSPACE_COUNT_CHANGED message.
    var message = firstWorkspace.waitForMessageOfType(MessageType.WORKSPACE_COUNT_CHANGED, 1000);
    var workspacesChangedBody = (WorkspacesChangedBody) message.body();
    assertEquals(1, workspacesChangedBody.workspaceCount());

    // Now connect a second workspace.
    var secondWorkspace = connectWorkspace(true, false);

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

    // that will be considered a known workspace
    expectKnownWorkspacePids(mockWorkspaceProcess.pid);

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

    // that has had a successful handshake
    setAuthToken("valid-token");

    // that will be considered a known workspace
    expectKnownWorkspacePids(mockWorkspaceProcess.pid);

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
  public void testSendingInvalidMessageStructureClosesSession(boolean sayHelloFirst) {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(sayHelloFirst, false);

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
  public void testSendingRandomMessageFirstClosesSession() {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false, false);

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

  /**
   * Test sending a WORKSPACE_HELLO message with an originator+payload pid mismatch.
   */
  @Test
  public void testSendingHelloMessageWithMismatchedOriginator() {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false, false);

    // When the workspace sends a WORKSPACE_HELLO message with wrong originator value. It should
    // match the body's payload (and be known to the sidecar).
    var message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_HELLO, "1234", "message-id-here"),
        new HelloBody(connectedWorkspace.mockWorkspaceProcess().pid.id())
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

  /**
   * Test sending a WORKSPACE_HELLO message with an originator+payload pid mismatch.
   */
  @Test
  public void testSendingHelloMessageWithUnknownProcessId() {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(false, false);

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

  /**
   * Test some error cases within WebsocketEndpoint::onMessage() when the originator header value
   * is wrong given the sending workspace session.
   */
  @ValueSource(strings = {
      "not-a-valid-pid",
      "sidecar",
      "1234" // an unknown workspace id
  })
  @ParameterizedTest
  public void testWrongOriginatorValueInMessageClosesSession(String invalidOriginator) {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(true, true);

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
  public void testOnMessageHandlingUnknownSession() {

    Log.infof("Test: testOnMessageHandlingUnknownSession, websocketEndpoint: %s", websocketEndpoint);
    Log.infof("Test: testOnMessageHandlingUnknownSession, websocketEndpoint sessions: %s", System.identityHashCode(websocketEndpoint.sessions));

    // Test the case where a message is received from a session that is not known to the sidecar.
    // (which should be impossible, but we should handle it gracefully.)
    ConnectedWorkspace connectedWorkspace = connectWorkspace(true, true);

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
  public void testWorkspaceBroadcastingWhenNoOtherWorkspaces() throws InterruptedException {
    // Given a workspace connected happy websocket ...
    var connectedWorkspace = connectWorkspace(true, true);

    // clear any messages received so far.
    // connectedWorkspace.clearReceivedMessages();

    // When the workspace sends a message that should be broadcast to all other workspaces ...
    var message = new Message(
        new MessageHeaders(MessageType.UNKNOWN, connectedWorkspace.processIdString(), "message-id-here"),
        new DynamicMessageBody(Map.of("foonly", 3))
    );
    connectedWorkspace.send(message);

    // then all is well. The session should still be open and the message we sent
    // should not have been received by the workspace itself.
    Thread.sleep(1000);

    assertTrue(connectedWorkspace.session().isOpen());
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
  public void testSendingMismatchedBodyType() {
    // Given a workspace connected happy websocket ...
    ConnectedWorkspace connectedWorkspace = connectWorkspace(false, false);

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
  public void testDisconnectingBeforeErrorSent() throws InterruptedException {
    // Send a bad hello message, but disconnect before the error message is sent.
    // Gets coverage over sendErrorAndCloseSession() error paths.
    ConnectedWorkspace connectedWorkspace = connectWorkspace(false, false);
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
    var connectedWorkspace = connectWorkspace(true, true);
    var firstWorkspacePid = connectedWorkspace.processId();

    // Now make second websocket connection, saying hello with the same pid as from the first
    // (in the body of the message, not the header. We want to get to a later error than if
    // the wrong pid was in the header.)
    var connectedWorkspace2 = connectWorkspace(false, false);
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
    assertTrue(connectedWorkspace.session().isOpen());

    // close the session so we don't leave it hanging around polluting other tests.
    connectedWorkspace.closeWebsocket();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testOnError(boolean shouldFindSession) throws IOException {
    // Connect as a workspace to establish a session, then directly call onError() with the
    // server side session handle.
    var connectedWorkspace = connectWorkspace(true, true);

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

  @Test
  public void testPurgeInactiveSessions() {

    var sessions = websocketEndpoint.sessions;
    // Calling when empty connection map should do nothing.
    websocketEndpoint.purgeInactiveSessions();

    // Given a workspace connected happy websocket ...
    ConnectedWorkspace connectedWorkspace = connectWorkspace(true, true);


    // Calling again should leave the session alone.
    websocketEndpoint.purgeInactiveSessions();
    assertEquals(1, sessions.size());

    // But make a second connection which does not say hello.
    ConnectedWorkspace connectedWorkspace2 = connectWorkspace(false, false);

    // find the inactive session in server-side sessions map ...
    var existingInactiveSession = sessions.values().stream().filter(
        session -> !session.isActive()
    ).findFirst().get();

    // .., and replace it with a session that is older than the pre-grace period.
    var maxAllowedSeconds = websocketEndpoint.initialGraceSeconds.get();

    var olderSession = new WorkspaceWebsocketSession(
        existingInactiveSession.session(),
        existingInactiveSession.workspacePid(),
        Instant.now().minusSeconds(maxAllowedSeconds + 1) // slightly too old!
    );
    sessions.put(existingInactiveSession.key(), olderSession);

    // Should be two entries still ...
    assertEquals(2, sessions.size());

    // Now groom the sessions. Will send an error message to the older session and close it,
    // then ultimately will remove it from the sessions map.
    websocketEndpoint.purgeInactiveSessions();

    // get the error message.
    var closureMessage = connectedWorkspace2.waitForMessageOfType(MessageType.PROTOCOL_ERROR, 1000);

    // expect closing.
    connectedWorkspace2.waitForClose(1000);

    // Should be only one entry left, not the older session.
    assertEquals(1, sessions.size());
    assertFalse(sessions.containsKey(existingInactiveSession.key()));
  }
}
