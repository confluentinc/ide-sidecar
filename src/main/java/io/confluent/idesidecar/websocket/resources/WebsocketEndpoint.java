package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.websocket.messages.HelloBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageBody;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ProtocolErrorBody;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.logging.Log;
import jakarta.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Websocket endpoint for "control plane" variety async messaging between sidecar and workspaces,
 * and/or workspace -> workspaces.
 */
@ServerEndpoint("/ws")
@ApplicationScoped
public class WebsocketEndpoint {
  /**
   * Map of active, authorized workspace sessions, keyed by the websocket session object.
   */
  @VisibleForTesting
  final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<>();

  /**
   * Set of sessions which have connected, but not yet sent a valid hello message.
   * The time that they connected is stashed as session.getUserProperties().get("connectedAt")
   * so that they can be purged after a certain amount of time if they linger in this state.
   */
  @VisibleForTesting
  final Set<Session> purgatorySessions = Collections.synchronizedSet(new HashSet<>());

  /**
   * Authority on the known workspaces in the system. Used to validate workspace ids.
   */
  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  private final ObjectMapper mapper = JsonMapper
      .builder()
      .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
      .build();

  /**
   * Broadcast a message originating from sidecar to all authorized connected workspaces.
   *
   * @param message the message to broadcast
   * @throws IOException              if there is an error serializing the message to JSON.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting.
   * */
  public void broadcast(Message message) throws IOException, IllegalArgumentException {
    final MessageHeaders headers = validateHeadersForSidecarBroadcast(message);

    if (sessions.isEmpty()) {
      Log.debug("No workspaces to broadcast message to.");
      return;
    }

    // Serialize the message to JSON
    String jsonMessage = mapper.writeValueAsString(message);
    Log.debugf(
        "Broadcasting %d char message, id %s to all workspaces",
        jsonMessage.length(),
        headers.id()
    );

    // And broadcast the message
    sessions.entrySet().stream()
        .filter(pair -> pair.getKey().isOpen())
        .forEach(pair -> {
          Log.debugf(
              "Sending broadcast message %s to workspace: %d",
              headers.id(),
              pair.getValue().processId()
          );
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /**
   * Handler for new websocket sessions.
   *
   * <p>When a new websocket session is opened, the workspace process id should be passed as a
   * request parameter. If it is not, the session is closed. If the workspace id is not known to
   * the sidecar {@link KnownWorkspacesBean}, the session is closed. Otherwise, a new
   * {@link WorkspaceSession} object is created and stored in the sessions map, and the updated
   * current connection count is broadcast to all workspace connections (inclusive).
   *
   * @param session      the session that was opened
   * @throws IOException if there is an error opening the session, or closing the session if the
   *                     workspace id is invalid or not provided.
   */
  @OnOpen
  public void onOpen(Session session) throws IOException {
    Log.infof("New websocket session opened, placed into purgatory: %s", session.getId());

    // Request must have had a valid access token to pass through AccessTokenFilter,
    // so we can assume that the session is authorized.
    // The workspace process id will come to us in a subsequent HELLO_WORKSPACE
    // message. For now, we stash the session into our purgatory set.
    var whenConnected = System.currentTimeMillis();
    // todo have a task that periodically checks purgatory sessions and closes them if they linger too long
    session.getUserProperties().put("connectedAt", whenConnected);
    purgatorySessions.add(session);
  }

  /**
   * Handle an incoming message from a websocket session. Called by the websocket server
   * when a message is received.
   * @param messageString The message as a JSON string, which will be parsed as a
   *                      {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @param senderSession The websocket session that sent the message.
   */
  @OnMessage
  public void onMessage(String messageString, Session senderSession) {
    var workspaceSession = sessions.get(senderSession);
    if (workspaceSession == null) {
      // Expect to find them in purgatory, and that this is a HELLO_WORKSPACE message
      if (!purgatorySessions.remove(senderSession)) {
        sendErrorAndCloseSession(
            senderSession,
            null,
            "Odd! Received message from unregistered session %s. Closing session.",
            senderSession.getId()
        );
        return;
      }

      // OK, was found in purgatory. Only expect a hello message when in this state.
      // Defer to internal handler for that sort of message.
      handleHelloMessage(senderSession, messageString);

      return;
    }

    // Message is from a known workspace session.
    Message m = deserializeMessage(senderSession, messageString, null);

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as
    // the originator)

    if (!validateOriginator(senderSession, m, workspaceSession.processId())) {
      return;
    }

    Log.debugf(
        "Received message %s of messageType %s from workspace %d",
        m.id(),
        m.messageType(),
        workspaceSession.processId()
    );

    // At this time, all websocket messages received from workspaces are intended to
    // be proxied to all the other workspaces. Do so here.

    var otherCount = sessions.size() - 1;
    if (otherCount <= 0) {
      Log.debug("No other workspaces to broadcast message to.");
    } else {
      Log.debugf(
          "Proxying message %s from workspace %d to %d other workspace(s)",
          m.id(),
          workspaceSession.processId(),
          otherCount
      );
      sessions
          .entrySet()
          .stream()
          .filter(pair ->
              pair.getValue().processId() != workspaceSession.processId() && pair.getKey().isOpen()
          )
          .forEach(pair ->
              pair.getKey().getAsyncRemote().sendText(messageString)
          );
    }
  }

  /**
   * Handle the first message from a workspace session, currently in purgatory,
   * which should be a HELLO_WORKSPACE message.
   *
   * If it is a valid HELLO_WORKSPACE message, the workspace is authorized and added to the
   * sessions map, and the new workspace count is broadcast to all workspaces (inclusive).
   * Otherwise an error message is sent to the session and it is closed.
   *
   * The session is always removed from the purgatory set.
   *
   * @param session the session that sent the message.
   * @param messageString the JSON string message.
   */
  private void handleHelloMessage(Session session, String messageString) {
    // Regardless of if the message was good or bad, we remove the session from purgatory
    // upon receipt of the first message.
    purgatorySessions.remove(session);

    // deserializeMessage() will close the connection if the message is invalid and
    // not a HELLO_WORKSPACE message
    var message = deserializeMessage(session, messageString, MessageType.WORKSPACE_HELLO);
    if (message == null) {
      Log.errorf(
          "handleHelloMessage: Invalid message from purgatory session %s. Closed session.",
          session.getId()
      );
    }

    // Message is valid and is a HELLO_WORKSPACE message with a HelloBody.
    var workspaceId = ((HelloBody) message.body()).workspaceId();

    // Ensure that the header origin value corresponds to the
    // workspace id in the body.
    if (!validateOriginator(session, message, workspaceId)) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "handleHelloMessage: Workspace id %s does not match originator value. Closing session.",
          workspaceId
      );
      return;
    }

    // Cross-reference the workspace id against the known workspaces.

    if (!knownWorkspacesBean.isKnownWorkspace(Long.valueOf(workspaceId))) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "handleHelloMessage: Unknown workspace id %d. Closing session.",
          workspaceId
      );
      return;
    }

    // Should be unique across all workspaces in session map.
    for(var ws : sessions.values()) {
      if (ws.processId() == workspaceId) {
        sendErrorAndCloseSession(
            session,
            message.headers().id(),
            "Workspace id %s already connected. Closing session.",
            workspaceId
        );
        return;
      }
    }

    // All good! Add the workspace to the sessions map and announce the count change.
    sessions.put(session, new WorkspaceSession(workspaceId));
    Log.infof("Workspace %d authorized and added to sessions.", workspaceId);

    // Announce to all other workspaces the list has changed
    try {
      broadcastWorkspacesChanged();
    } catch (IOException e) {
      Log.errorf("Failed to broadcast workspace added message: %s", e.getMessage());
    }
  }

  @OnError
  public void onError(Session session, Throwable throwable) throws IOException {
    // May or may not actually remove -- if had not yet been authorized, it won't be in the map.
    // (but will definitely not be in the map after this statement.)
    var existingSession = sessions.remove(session);
    session.close();

    var id = existingSession != null ? existingSession.processId() : "unknown";
    Log.errorf(
        "Websocket error for workspace pid %s, session id %s. Reason: %s. Closed and removed session.",
        id,
        session.getId(),
        throwable.getMessage()
    );
  }

  @OnClose
  public void onClose(Session session) {
    var existing = sessions.remove(session);
    Log.info("Websocket session closed: " + session.getId());

    if (existing != null) {
      // was a registered workspace session. Announce to all other workspaces the list has changed
      Log.infof(
          "Closed session was workspace %d, broadcasting workspace count change.",
          existing.processId()
      );
      try {
        broadcastWorkspacesChanged();
      } catch (IOException e) {
        Log.errorf("Failed to broadcast workspace removed message: %s", e.getMessage());
      }
    } else {
      // Must have been in purgatory and hung up before saying hello.
      purgatorySessions.remove(session);
    }
  }

  /**
   * Send a message to all workspaces that the count of authorized workspaces has changed.
   * Used whenever a workspace is added or removed.
   */
  private void broadcastWorkspacesChanged() throws IOException {
    // changedWorkspace was either just added or removed. Inform all workspaces about the
    // new connected/authorized workspace count.

    var message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, "sidecar"),
        new WorkspacesChangedBody(this.sessions.size())
    );

    Log.info("Broadcasting workspace count change message to all workspaces...");

    broadcast(message);
  }

  /**
   * Send an PROTOCOL_ERROR message to a websocket session, then close the session.
   * @param session the session to send the error message to.
   * @param message the error message to send.
   */
  private void sendErrorAndCloseSession(
      Session session,
      String originalMessageId,
      String message,
      Object...messageParams
  ) {
    // Remove the session if it exists
    sessions.remove(session);

    // Send the error message to the session
    var msg = message.formatted(messageParams);
    Log.info(msg);
    try {
      var errorMessage = new Message(
          new MessageHeaders(MessageType.PROTOCOL_ERROR, "sidecar"),
          new ProtocolErrorBody(msg, originalMessageId)
      );
      session.getAsyncRemote()
             .sendText(mapper.writeValueAsString(errorMessage));
    } catch (IOException e) {
      Log.errorf(
          "Unable to send error message to session %s: %s",
          session.getId(),
          e.getMessage(),
          e
      );
    } finally {
      try {
        // And always close the session
        session.close();
      } catch (IOException e) {
        Log.errorf(
            "Unable to close session %s: %s",
            session.getId(),
            e.getMessage(),
            e
        );
      }
    }
  }

  /**
   * Validate that the headers are suitable for a broadcast sidecar -> all workspaces message.
   * @param outboundMessage the message intended to be sent.
   * @return the validated headers of the message.
   * @throws IllegalArgumentException if the headers are not suitable for broadcasting.
   */
  @VisibleForTesting
  static MessageHeaders validateHeadersForSidecarBroadcast(Message outboundMessage) {
    MessageHeaders headers = outboundMessage.headers();

    if (!headers.originator().equals("sidecar")) {
      Log.errorf(
          "Message id %s is not originator=sidecar message, cannot broadcast.",
          headers.id()
      );
      throw new IllegalArgumentException(
          "Attempted to broadcast a non-sidecar message to workspaces."
      );
    }
    return headers;
  }

  /**
   * Deserialize a message from a JSON string, and optionally validate that
   * it is of the expected type and body payload.
   * If the message is invalid, an error message is sent to the session, the session is closed, and
   * null is returned.
   * @param session the session that sent the message.
   * @param messageString the JSON string message.
   * @param expectedType the expected message type.
   * @return the deserialized message, or null if the message is invalid.
   */
  @VisibleForTesting
  Message deserializeMessage(
      @NotNull Session session,
      @NotNull String messageString,
      MessageType expectedType
  ) {
    Message m;
    try {
      m = mapper.readValue(messageString, Message.class);
    } catch (IOException e) {
      sendErrorAndCloseSession(
          session,
          null,
          "Unparseable message from session %s. Discarding and closing.",
          session.getId()
      );
      return null;
    }

    // Optional check against message type and deserialized payload body
    if (expectedType != null) {
      var headers = m.headers();
      if (headers.messageType() != expectedType) {
        sendErrorAndCloseSession(
            session,
            headers.id(),
            "Expected %s message, got %s. Closing session.",
            expectedType,
            headers.messageType()
        );
        return null;
      }

      var expectedBodyType = expectedType.bodyClass();
      if (expectedBodyType != null) {
        var body = m.body();
        if (!expectedBodyType.isInstance(body)) {
          sendErrorAndCloseSession(
              session,
              headers.id(),
              "Expected %s message body, got %s. Closing session.",
              expectedBodyType.getSimpleName(),
              body.getClass().getSimpleName()
          );
          return null;
        }
      }
    }

    // Message is valid.
    return m;
  }

  /**
   * Validate originator message header vs expected value.
   * If invalid, will send an error message to the session, remove it from sessions map,
   * and close the session, then return false.
   * If valid, will return true
   *
   * @param message the message to validate.
   */
  private boolean validateOriginator(Session session, Message message, long expectedWorkspaceId) {
    var claimedWorkspaceId = 0;
    try {
      claimedWorkspaceId = Integer.parseInt(message.headers().originator());
    } catch (NumberFormatException e) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "Invalid websocket message header originator value -- not an integer: %s. "
              + "Removing and closing session %s.",
          message.messageType(),
          session.getId()
      );
      return false;
    }

    if (claimedWorkspaceId != expectedWorkspaceId) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "Workspace %s sent message with incorrect originator value: %d."
              + " Removing and closing session %s.",
          expectedWorkspaceId,
          claimedWorkspaceId,
          session.getId()
      );
      return false;
    }

    return true;
  }
}
