package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ProtocolErrorBody;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.logging.Log;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

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
    Log.info("New websocket session opened: " + session.getId());

    // Request must have had a valid access token to pass through AccessTokenFilter,
    // so we can assume that the session is authorized.
    // The workspace process id should have been passed as a request parameter, though.
    var requestParams = session.getRequestParameterMap();
    if (requestParams.size() == 0) {
      sendErrorAndCloseSession(
          session,
          null,
          "No request parameters provided. Closing new session %s.",
          session.getId()
      );
      return;
    }

    var workspaceIdList = requestParams.get("workspace_id");
    if (workspaceIdList == null || workspaceIdList.isEmpty()) {
      sendErrorAndCloseSession(
          session,
          null,
          "No workspace_id parameter provided. Closing new session %s.",
          session.getId()
      );
      return;
    }

    var workspaceIdString = workspaceIdList.get(0);
    long workspaceId;
    try {
      workspaceId = Long.parseLong(workspaceIdString);
    } catch (NumberFormatException e) {
      sendErrorAndCloseSession(
          session,
          null,
          "Invalid workspace_id parameter value: %s. Closing new session %s.",
          workspaceIdString,
          session.getId()
      );
      return;
    }

    // As of time of writing, the workspace should have REST handshook or hit the health check
    // route with the workspace id header, so we should know about it already.
    if (!knownWorkspacesBean.isKnownWorkspacePID(workspaceId)) {
      sendErrorAndCloseSession(
          session,
          null,
          "Unknown workspace id: %d. Closing new session %s.",
          workspaceId,
          session.getId()
      );
      return;
    }

    Log.infof(
        "New websocket session %s opened for workspace pid: %d",
        session.getId(),
        workspaceId
    );
    // create new WorkspaceSession object and store in sessions map.
    var newWorkspaceSession = new WorkspaceSession(workspaceId);
    sessions.put(session, newWorkspaceSession);

    // Broadcast message to all workspaces (inclusive) a change in the authorized workspaces count.
    broadcastWorkspacesChanged();
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
      sendErrorAndCloseSession(
          senderSession,
          null,
          "Odd! Received message from unregistered session %s. Closing session.",
          senderSession.getId()
      );
      return;
    }

    Message m;
    try {
      m = mapper.readValue(messageString, Message.class);
    } catch (IOException e) {
      sendErrorAndCloseSession(
          senderSession,
          null,
          "Invalid message from session %s, workspace %d. Discarding and closing.",
          senderSession.getId(),
          workspaceSession.processId()
      );
      return;
    }

    var headers = m.headers();

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as
    // the originator)
    var claimedWorkspaceId = 0;
    try {
      claimedWorkspaceId = Integer.parseInt(headers.originator());
    } catch (NumberFormatException e) {
      sendErrorAndCloseSession(
          senderSession,
          m.headers().id(),
          "Invalid websocket message header originator value -- not an integer: %s. "
          + "Removing and closing session %s.",
          headers.messageType(),
          senderSession.getId()
      );
      return;
    }

    if (claimedWorkspaceId != workspaceSession.processId()) {
      sendErrorAndCloseSession(
          senderSession,
          m.headers().id(),
          "Workspace %s sent message with incorrect originator value: %d."
          + " Removing and closing session %s.",
          workspaceSession.processId(),
          claimedWorkspaceId,
          senderSession.getId()
      );
      return;
    }

    Log.debugf(
        "Received message %s of messageType %s from workspace %d",
        headers.id(),
        headers.messageType(),
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
          headers.id(),
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

  @OnError
  public void onError(Session session, Throwable throwable) throws IOException {
    // May or may not actually remove -- if had not yet been authorized, it won't be in the map.
    // (but will definitely not be in the map after this statement.)
    var existingSession = sessions.remove(session);
    session.close();

    var id = existingSession != null ? existingSession.processId() : "unknown";
    Log.errorf(
        "Websocket error for workspace pid %s, session id %s. Closed and removed session.",
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
}
