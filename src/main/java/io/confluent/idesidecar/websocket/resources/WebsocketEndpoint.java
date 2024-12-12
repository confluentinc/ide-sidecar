package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.websocket.messages.Audience;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ResponseMessageHeaders;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Provider;
import jakarta.inject.Inject;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ServerEndpoint("/ws")
@ApplicationScoped
public class WebsocketEndpoint {


  /**
   * Map of active, authorized workspace sessions, keyed by the websocket session object.
   */
  private final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<Session, WorkspaceSession>();


  // Miscellany
  /** Jackson object mapper for serializing/deserializing messages. */
  private final ObjectMapper mapper = new ObjectMapper();
  /** Logger for this class. */
  private static final Logger log = LoggerFactory.getLogger(WebsocketEndpoint.class);


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
      log.debug("No workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + headers.id + " to all workspaces");

    sessions.entrySet().stream()
        .filter(pair -> pair.getKey().isOpen())
        .forEach(pair -> {
          log.debug("Sending broadcasted message " + headers.id + " to workspace: " + pair.getValue().processId());
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
      log.error("Odd! Received message from unregistered session. Closing session.");
      senderSession.close();
      return;
    }

    Message m;

    try {
      m =  parseAndValidateMessage(messageString);
    } catch (java.io.IOException e) {
      log.error("Invalid message from workspace: " + workspaceSession.processId() + ", closing session and discarding.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    MessageHeaders headers = m.getHeaders();

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as the originator)
    int claimedWorkspaceId = 0;
    try {
      claimedWorkspaceId = Integer.parseInt(headers.originator);
    } catch (NumberFormatException e) {
      log.error("Invalid websocket message header originator value -- not an integer: " + headers.originator + ". Removing and closing session.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    if (claimedWorkspaceId != workspaceSession.processId()) {
      log.error("Workspace " + workspaceSession.processId() + " sent message with incorrect originator value: " + claimedWorkspaceId + ".Removing and closing session.");
      sessions.remove(senderSession);
      senderSession.close();
      return;
    }

    log.debug("Received " + headers.type + " message from workspace: " + workspaceSession.processId());

    // Handle the message based on its audience.
    if (headers.audience == Audience.workspaces)
    {
      // Message is intended for all (other) workspaces, using sidecar as a broadcast bus
      // for workspace-to-all-other-workspace messages.
      int otherCount = sessions.size() - 1;
      if (otherCount == 0) {
        log.debug("No other workspaces to broadcast message to.");
        return;
      } else {
        log.debug("Broadcasting message to " + otherCount + " other workspaces");
        broadcast(m, workspaceSession);
      }

    } else if (headers.audience == Audience.sidecar) {
      // Message must be intended for sidecar

      // todo defer to an internal message router here.

      log.error("Unexpected message audience: " + m.getHeaders().audience);
    } else {
      // We don't (yet) support or design for directed workspace -> workspace messages.
      log.error("Unhandled message audience: " + m.getHeaders().audience);
    }
  }

  @OnOpen
  public void onOpen(Session session) throws IOException {
    // Don't store into sessions map until successful handling of ACCESS_REQUEST message,
    // so do nothing of importance here.
    log.info("New websocket session opened: " + session.getId());
    // Request must have had a valid access token to pass through AccessTokenFilter, so we can assume that the session is authorized.
    // The workspace process id should have been passed as a request parameter, though.
    String workspaceIdString = session.getRequestParameterMap().get("workspace_id").get(0);

    if (workspaceIdString == null) {
      log.error("No workspace_id parameter provided. Closing session.");
      session.close();
      return;
    }

    int workspaceId;
    try {
      workspaceId = Integer.parseInt(workspaceIdString);
    } catch (NumberFormatException e) {
      log.error("Invalid workspace_id parameter value: " + workspaceIdString + ". Closing session.");
      session.close();
      return;
    }

    log.info("New websocket session opened for workspace pid: " + workspaceId);
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
    log.error(logPrefix + throwable.getMessage());
    log.error(logPrefix + "Session removed.");
  }

  @OnClose
  public void onClose(Session session) {
    WorkspaceSession existing = sessions.remove(session);
    log.info("Websocket session closed: " + session.getId());

    if (existing != null) {
      // was a registered workspace session. Announce to all other workspaces that the list has changed.
      log.info("Closed session was workspace " + existing.processId() + ", broadcasting workspace count change.");
      try {
        broadcastWorkspacesChanged();
      } catch (java.io.IOException e) {
        log.error("Failed to broadcast workspace removed message: " + e.getMessage());
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
    if (!headers.originator.matches("\\d+")) {
      throw new IOException("Invalid websocket message header originator value: " + headers.originator);
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
        new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, Audience.workspaces, "sidecar"),
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
      log.debug("No other workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + headers.id + " from workspace: " + sender.processId());

    sessions.entrySet().stream()
        .filter(pair -> pair.getValue().processId() != sender.processId() && pair.getKey().isOpen())
        .forEach(pair -> {
          log.debug("Sending broadcasted message " + headers.id + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /** Send a directed message from sidecar to a specific websocket session. */
  private void sendMessage(Session recipient, Message message) throws java.io.IOException {
    String jsonMessage = mapper.writeValueAsString(message);
    log.info("Sending " + jsonMessage.length() + " char message, id " + message.getId() + " to workspace: " + recipient.getId());
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

    if (headers instanceof ResponseMessageHeaders) {
      log.error("Message id " + headers.id + " has a reponse id, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a response message to workspaces.");
    }

    if (headers.audience != Audience.workspaces) {
      log.error("Message id " + headers.id + " is not audience=workspaces message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-workspaces message to workspaces.");
    }

    if (! headers.originator.equals("sidecar")) {
      log.error("Message id " + headers.id + " is not originator=sidecar message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-sidecar message to workspaces.");
    }

    return headers;
  }
}
