package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.logging.Log;
import java.io.IOException;
import java.util.List;
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
import javax.validation.constraints.NotNull;

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
  private final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<>();

  /**
   * Authority on the known workspaces in the system. Used to validate workspace ids.
   */
  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  // Miscellany
  private final ObjectMapper mapper = JsonMapper
      .builder()
      .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
      .build();

  /**
   * Broadcast a message originating from sidecar to all authorized connected workspaces.
   *
   * @throws IOException              if there is an error serializing the message to JSON.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting.
   * */
  public void broadcast(Message message) throws IOException {
    final MessageHeaders headers = validateHeadersForSidecarBroadcast(message);

    if (sessions.isEmpty()) {
      Log.debug("No workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    Log.debugf(
        "Broadcasting %d char message, id %s to all workspaces",
        jsonMessage.length(),
        headers.id()
    );

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
   * @param session      the session that was opened
   * @throws IOException if there is an error opening the session, or closing the session if the
   *                     workspace id is invalid or not provided.
   */
  @OnOpen
  public void onOpen(Session session) throws IOException {
    Log.info("New websocket session opened: " + session.getId());

    // Request must have had a valid access token to pass through AccessTokenFilter, so we can assume that the session is authorized.
    // The workspace process id should have been passed as a request parameter, though.
    Map <String, java.util.List<String>> requestParams = session.getRequestParameterMap();
    if (requestParams.size() == 0) {
      Log.error("No request parameters provided. Closing session.");
      session.close();
      return;
    }

    List<String> workspaceIdList = requestParams.get("workspace_id");
    if (workspaceIdList == null || workspaceIdList.isEmpty()) {
      Log.error("No workspace_id parameter provided. Closing session.");
      session.close();
      return;
    }

    String workspaceIdString = workspaceIdList.get(0);
    long workspaceId;
    try {
      workspaceId = Long.parseLong(workspaceIdString);
    } catch (NumberFormatException e) {
      Log.errorf("Invalid workspace_id parameter value: %s. Closing session.", workspaceIdString);
      session.close();
      return;
    }

    // As of time of writing, the workspace should have REST handshook or hit the health check
    // route with the workspace id header, so we should know about it already.
    if (!knownWorkspacesBean.isKnownWorkspacePID(workspaceId)) {
      Log.errorf("Unauthorized workspace id: %d. Closing session.", workspaceId);
      session.close();
      return;
    }

    Log.infof("New websocket session opened for workspace pid: %d", workspaceId);
    // create new WorkspaceSession object and store in sessions map.
    WorkspaceSession newWorkspaceSession = new WorkspaceSession(workspaceId);
    sessions.put(session, newWorkspaceSession);

    // Broadcast a message to all workspaces (inclusive) that the authorized workspaces count has changed.
    broadcastWorkspacesChanged();
  }

  /**
   * Handle an incoming message from a websocket session. Called by the websocket server
   * when a message is received.
   * @param messageString The message as a JSON string, which will be parsed as a
   *                      {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @param senderSession The websocket session that sent the message.
   * @throws IOException if there is an error parsing the message or if the message is invalid
   */
  @OnMessage
  public void onMessage(String messageString, Session senderSession) throws IOException {
    WorkspaceSession workspaceSession = sessions.get(senderSession);
    if (workspaceSession == null) {
      Log.error("Odd! Received message from unregistered session. Closing session.");
      senderSession.close();
      return;
    }

    Message m;

    try {
      m =  parseAndValidateMessage(messageString);
    } catch (java.io.IOException e) {
      Log.errorf(
          "Invalid message from workspace: %d, closing session and discarding.",
          workspaceSession.processId()
      );
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    MessageHeaders headers = m.headers();

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as the originator)
    int claimedWorkspaceId = 0;
    try {
      claimedWorkspaceId = Integer.parseInt(headers.originator());
    } catch (NumberFormatException e) {
      Log.errorf(
          "Invalid websocket message header originator value -- not an integer: %s. "
          + "Removing and closing session.",
          headers.originator()
      );
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    if (claimedWorkspaceId != workspaceSession.processId()) {
      Log.errorf(
          "Workspace %s sent message with incorrect originator value: %d."
          + " Removing and closing session.",
          workspaceSession.processId(),
          claimedWorkspaceId
      );
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    Log.debugf(
        "Received message %s of messageType %s from workspace %d",
        headers.id(),
        headers.messageType(),
        workspaceSession.processId()
    );

    /*
      At this time, all websocket messages received from workspaces are intended to
      be proxied to all of the other workspaces. Do so here.
     */

    int otherCount = sessions.size() - 1;
    if (otherCount <= 0) {
      Log.debug("No other workspaces to broadcast message to.");
      return;
    } else {
      Log.debugf(
          "Proxying message %s from workspace %d to %d other workspace(s)",
          headers.id(),
          workspaceSession.processId(),
          otherCount
      );
      sessions.entrySet().stream()
          .filter(pair -> pair.getValue().processId() != workspaceSession.processId() && pair.getKey().isOpen())
          .forEach(pair -> {
            pair.getKey().getAsyncRemote().sendText(messageString);
          });
    }
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    // May or may not actually remove -- if had not yet been authorized, it won't be in the map.
    // (but will definitely not be in the map after this statement.)
    WorkspaceSession existingSession = sessions.remove(session);

    var id = existingSession != null ? existingSession : session.getId();
    String logMsg = "Websocket error for authorized workspace: %s - %s";
    Log.errorf(logMsg, id, throwable.getMessage());
    Log.errorf(logMsg, id, "Session removed.");
  }

  @OnClose
  public void onClose(Session session) {
    WorkspaceSession existing = sessions.remove(session);
    Log.info("Websocket session closed: " + session.getId());

    if (existing != null) {
      // was a registered workspace session. Announce to all other workspaces that the list has changed.
      Log.infof(
          "Closed session was workspace %d, broadcasting workspace count change.",
          existing.processId()
      );
      try {
        broadcastWorkspacesChanged();
      } catch (java.io.IOException e) {
        Log.errorf("Failed to broadcast workspace removed message: %s", e.getMessage());
      }
    }
  }

  /**
   * Parse and validate a message from a websocket session. If the message is not parseable or
   * fails coarse validation,IOException is raised
   * @param message JSON spelling of a {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @return The parsed message
   * @throws java.io.IOException on any error.
   */
  @NotNull
  private Message parseAndValidateMessage(String message) throws java.io.IOException {
    Message m = mapper.readValue(message, Message.class);

    MessageHeaders headers = m.headers();

    // header.originator for messages recv'd by sidecar must always be a string'd integer
    // representing the workspace id (process id).
    if (!headers.originator().matches("\\d+")) {
      throw new IOException(
          "Invalid websocket message header originator value: " + headers.originator()
      );
    }

    return m;
  }

  /**
   * Send a message to all workspaces that the count of authorized workspaces has changed.
   * Used whenever a workspace is added or removed.
   */
  private void broadcastWorkspacesChanged() throws java.io.IOException {
    // changedWorkspace was either just added or removed. Informall  workspaces about the new connected/authorized workspace count.

    Message message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, "sidecar"),
        new WorkspacesChangedBody(this.sessions.size())
    );

    Log.info("Broadcasting workspace count change message to all workspaces...");

    broadcast(message);
  }

  /**
   * Validate that the headers are suitable for a broadcasted sidecar -> all workspaces message.
   * @param outboundMessage the message intended to be sent.
   * @return the validated headers of the message.
   * @throws IllegalArgumentException if the headers are not suitable for broadcasting.
   */
  @VisibleForTesting
  static MessageHeaders validateHeadersForSidecarBroadcast(Message outboundMessage) {
    MessageHeaders headers = outboundMessage.headers();

    if (! headers.originator().equals("sidecar")) {
      Log.errorf("Message id %s is not originator=sidecar message, cannot broadcast.", headers.id());
      throw new IllegalArgumentException(
          "Attempted to broadcast a non-sidecar message to workspaces."
      );
    }

    return headers;
  }
}
