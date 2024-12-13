package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;

import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;
import io.quarkus.logging.Log;


@ServerEndpoint("/ws")
@ApplicationScoped
public class WebsocketEndpoint {


  /**
   * Map of active, authorized workspace sessions, keyed by the websocket session object.
   */
  private final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<Session, WorkspaceSession>();

  /**
   * Authority on the known workspaces in the system. Used to validate workspace ids.
   */
  @Inject
  KnownWorkspacesBean knownWorkspacesBean;


  // Miscellany
  /** Jackson object mapper for serializing/deserializing messages. */
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Broadcast a message originating from sidecar to all authorized connected workspaces.
   *
   * @throws java.io.IOException if there is an error serializing the message to JSON.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting.
   * */
  public void broadcast(Message message) throws java.io.IOException {
    // Expected to be usable from other parts of the sidecar codebase.

    final MessageHeaders headers = validateHeadersForSidecarBroadcast(message);

    if (sessions.isEmpty()) {
      Log.debug("No workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    Log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + headers.id() + " to all workspaces");

    sessions.entrySet().stream()
        .filter(pair -> pair.getKey().isOpen())
        .forEach(pair -> {
          Log.debug("Sending broadcasted message " + headers.id() + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /**
   * Handle an incoming message from a websocket session. Called by the websocket server when a message is received.
   * @param messageString The message as a JSON string. Should deserialize to an {@link io.confluent.idesidecar.websocket.messages.Message} object.
   * @param senderSession The websocket session that sent the message.
   * @throws java.io.IOException
   */
  @OnMessage
  public void onMessage(String messageString, Session senderSession) throws java.io.IOException {
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
      Log.error("Invalid message from workspace: " + workspaceSession.processId() + ", closing session and discarding.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    MessageHeaders headers = m.getHeaders();

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as the originator)
    int claimedWorkspaceId = 0;
    try {
      claimedWorkspaceId = Integer.parseInt(headers.originator());
    } catch (NumberFormatException e) {
      Log.error("Invalid websocket message header originator value -- not an integer: " + headers.originator() + ". Removing and closing session.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    if (claimedWorkspaceId != workspaceSession.processId()) {
      Log.error("Workspace " + workspaceSession.processId() + " sent message with incorrect originator value: " + claimedWorkspaceId + ".Removing and closing session.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    Log.debug("Received " + headers.type() + " message from workspace: " + workspaceSession.processId());

    // At this time, all messages recieved from workspaces are intended to be broadcasted to
    // all other workspaces.
    int otherCount = sessions.size() - 1;
    if (otherCount == 0) {
      Log.debug("No other workspaces to broadcast message to.");
      return;
    } else {
      Log.debug("Broadcasting message to " + otherCount + " other workspaces");
      broadcast(m, workspaceSession);
    }

  }

  @OnOpen
  public void onOpen(Session session) throws IOException {
    Log.info("New websocket session opened: " + session.getId());

    // Request must have had a valid access token to pass through AccessTokenFilter, so we can assume that the session is authorized.
    // The workspace process id should have been passed as a request parameter, though.
    String workspaceIdString = session.getRequestParameterMap().get("workspace_id").get(0);

    if (workspaceIdString == null) {
      Log.error("No workspace_id parameter provided. Closing session.");
      session.close();
      return;
    }

    long workspaceId;
    try {
      workspaceId = Long.parseLong(workspaceIdString);
    } catch (NumberFormatException e) {
      Log.error("Invalid workspace_id parameter value: " + workspaceIdString + ". Closing session.");
      session.close();
      return;
    }

    // As of time of writing, the workspace should have REST handshook or hit the health check
    // route with the workspace id header, so we should know about it already.
    if (!knownWorkspacesBean.isKnownWorkspacePID(workspaceId)) {
      Log.error("Unauthorized workspace id: " + workspaceId + ". Closing session.");
      session.close();
      return;
    }

    Log.info("New websocket session opened for workspace pid: " + workspaceId);
    // create new WorkspaceSession object and store in sessions map.
    WorkspaceSession newWorkspaceSession = new WorkspaceSession(workspaceId);
    sessions.put(session, newWorkspaceSession);

    // Broadcast a message to all workspaces (inclusive) that the authorized workspaces count has changed.
    broadcastWorkspacesChanged();

  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    // May or may not actually remove -- if had not yet been authorized, it won't be in the map.
    // (but will definitely not be in the map after this statement.)
    WorkspaceSession existingSession = sessions.remove(session);

    String logPrefix;
    if (existingSession != null) {
      logPrefix = "Websocket error for authorized workspace: " + existingSession.processId() + " - ";
    } else {
      logPrefix = "Websocket error for unauthorized session: " + session.getId() + " - ";
    }
    Log.error(logPrefix + throwable.getMessage());
    Log.error(logPrefix + "Session removed.");
  }

  @OnClose
  public void onClose(Session session) {
    WorkspaceSession existing = sessions.remove(session);
    Log.info("Websocket session closed: " + session.getId());

    if (existing != null) {
      // was a registered workspace session. Announce to all other workspaces that the list has changed.
      Log.info("Closed session was workspace " + existing.processId() + ", broadcasting workspace count change.");
      try {
        broadcastWorkspacesChanged();
      } catch (java.io.IOException e) {
        Log.error("Failed to broadcast workspace removed message: " + e.getMessage());
      }
    }
  }

  /**
   * Parse and validate a message from a websocket session. If the message is not parseable or
   * fails coarse validation,IOException is raised
   * @param message: JSON spelling of a {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @return: The parsed message
   * @throws java.io.IOException on any error.
   */
  @NotNull
  private Message parseAndValidateMessage(String message) throws java.io.IOException {
    Message m = mapper.readValue(message, Message.class);

    MessageHeaders headers = m.getHeaders();

    // header.originator for messages recv'd by sidecar must always be a string'd integer
    // representing the workspace id (process id).
    if (!headers.originator().matches("\\d+")) {
      throw new IOException("Invalid websocket message header originator value: " + headers.originator());
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

    broadcast(message);
  }

  /**
   * Broadcast this message originating from sidecar to all other authorized workspaces. Is what we do with all audience=="workspaces" messages,
   * either originating from other workspaces or perhaps from sidecar itself.
   *
   * Skips sending the message to the distinguished "sender" workspace.
   *
   * */
  private void broadcast(Message message, WorkspaceSession sender) throws java.io.IOException {
    final MessageHeaders headers = validateHeadersForSidecarBroadcast(message);

    if (sessions.isEmpty()) {
      Log.debug("No other workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    Log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + headers.id() + " from workspace: " + sender.processId());

    sessions.entrySet().stream()
        .filter(pair -> pair.getValue().processId() != sender.processId() && pair.getKey().isOpen())
        .forEach(pair -> {
          Log.debug("Sending broadcasted message " + headers.id() + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /** Send a directed message from sidecar to a specific websocket session. */
  private void sendMessage(Session recipient, Message message) throws java.io.IOException {
    String jsonMessage = mapper.writeValueAsString(message);
    Log.info("Sending " + jsonMessage.length() + " char message, id " + message.getId() + " to workspace: " + recipient.getId());
    recipient.getAsyncRemote().sendText(jsonMessage);
  }

  /**
   * Validate that the headers are suitable for a broadcasted sidecar -> all workspaces message.
   * @param outboundMessage the message intended to be sent.
   * @return the validated headers of the message.
   * @throws IllegalArgumentException if the headers are not suitable for broadcasting.
   */
  private MessageHeaders validateHeadersForSidecarBroadcast(Message outboundMessage) {
    MessageHeaders headers = outboundMessage.getHeaders();

    if (! headers.originator().equals("sidecar")) {
      Log.errorf("Message id %s is not originator=sidecar message, cannot broadcast.", headers.id());
      throw new IllegalArgumentException("Attempted to broadcast a non-sidecar message to workspaces.");
    }

    return headers;
  }
}
