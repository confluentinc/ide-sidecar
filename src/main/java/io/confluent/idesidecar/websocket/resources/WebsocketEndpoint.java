package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.websocket.messages.Audience;
import io.confluent.idesidecar.websocket.messages.AccessRequestBody;
import io.confluent.idesidecar.websocket.messages.AccessResponseBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ResponseMessageHeaders;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
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

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ServerEndpoint("/ws")
@ApplicationScoped
public class WebsocketEndpoint {


  /**
   * Map of active, authorized workspace sessions, keyed by the websocket session. Sessions
   * are added when passed the ACCESS_REQUEST challenge and removed upon disconnect or error.
   * @see {@link #handleAccessRequestMessage}
   */
  private final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<Session, WorkspaceSession>();

  /**
   * Do we need to validate the access token presented in a ACCESS_REQUEST message at startup
   * of websocket connection?
   *
   * Same knob that controls whether the sidecar will validate an access token for REST API requests,
   * see {@link io.confluent.idesidecar.restapi.filters.AccessTokenFilter}.
   * */
  @ConfigProperty(name = "ide-sidecar.access_token_filter.enabled", defaultValue = "true")
  Provider<Boolean> authorization_required;

  /**
   * If authorization is required, where to get the access token to compare against.
   */
  @Inject
  SidecarAccessTokenBean accessTokenBean;

  // Miscellany
  /** Jackson object mapper for serializing/deserializing messages. */
  private final ObjectMapper mapper = new ObjectMapper();
  /** Logger for this class. */
  private static final Logger log = LoggerFactory.getLogger(WebsocketEndpoint.class);


  /**
   * Broadcast a message originating from sidecar to all authorized connected workspaces.
   * */
  public void broadcast(Message message) throws java.io.IOException {
    // Expected to be usable from other parts of the sidecar codebase.

    if (message.getHeaders().audience != Audience.workspaces) {
      log.error("Message id " + message.getId() + " is not a workspaces message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-workspaces message to workspaces.");
    }

    if (! message.getHeaders().originator.equals("sidecar")) {
      log.error("Message id " + message.getId() + " is not a sidecar message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-sidecar message to workspaces.");
    }

    if (sessions.isEmpty()) {
      log.debug("No workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + message.getId() + " to all workspaces");

    sessions.entrySet().stream()
        .filter(pair -> pair.getKey().isOpen())
        .forEach(pair -> {
          log.debug("Broadcasting message " + message.getId() + " to workspace: " + pair.getValue().processId());
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
      // The only message we expect from an unauthorized (not present in our map yet)
      // session is an access request message.
      handleAccessRequestMessage(senderSession, messageString);
      return;
    }

    // Handle all other message types ...
    Message m = parseAndValidateMessage(messageString);

    if (m == null) {
      log.error("Invalid message from workspace: " + workspaceSession.processId() + ", discarding.");
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
  public void onOpen(Session session) {
    // Don't store into sessions map until authorized, so do nothing of importance here.
    log.info("New websocket session opened: " + session.getId());
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
        broadcastWorkspacesChanged(existing);
      } catch (java.io.IOException e) {
        log.error("Failed to broadcast workspace removed message: " + e.getMessage());
      }
    }
  }

  /**
   * Parse and validate a message from a websocket session. If the message is not parseable or
   * fails validation, null is returned.
   * @param message: JSON spelling of a {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @return: The parsed message, or null if the message was invalid.
   */
  private Message parseAndValidateMessage(String message) {
    try {
      Message m = mapper.readValue(message, Message.class);

      MessageHeaders headers = m.getHeaders();

      // header.originator for messages recv'd by sidecar must always be a string'd integer
      if (!headers.originator.matches("\\d+")) {
        log.error("Invalid websocket message header originator value: " + headers.originator);
        return null;
      }

      return m;
    } catch (Exception e) {
      log.error("Failed to deserialize message: " + e.getMessage());
      return null;
    }
  }

  /**
   * Parse and validate an access request message from a newly connected workspace. If the
   * message type is not ACCESS_REQUEST, or the access token is invalid, the session is closed.
   * Otherwise, the session is stored in the sessions map and considered valid hereon out.
   *
   * @throws java.io.IOException
   */
  private void handleAccessRequestMessage(Session senderSession, String stringMessage) throws java.io.IOException {
    Message message = parseAndValidateMessage(stringMessage);
    if (message == null) {
      log.error("Invalid message, expected an ACCESS_REQUEST message, but was not parseable. Closing session.");
      senderSession.close();
      return;
    }

    if (!(message.getBody() instanceof AccessRequestBody body)) {
      log.error("Expected AccessRequestBody as the payload, got " + message.getBody().getClass().getName() + " instead. Closing session.");
      senderSession.close();
      return;
    }

    MessageHeaders headers = message.getHeaders();

    // The originator field for a workspace->sidecar message should be the workspace's (process) id.
    int actualWorkspaceId = 0;
    try {
      actualWorkspaceId = Integer.parseInt(headers.originator);
    } catch (NumberFormatException e) {
      log.error("Invalid websocket message header originator value -- not an integer: " + headers.originator + ". Closing session.");
      senderSession.close();
      return;
    }

    log.debug("Received authorization request from workspace pid: " + actualWorkspaceId);;

    // Same header will be used for the response message, be it success or failure.
    ResponseMessageHeaders responseHeaders =  new ResponseMessageHeaders (
        MessageType.ACCESS_RESPONSE,
        headers.id
    );

    if (!authorization_required.get()) {
      log.info("Websocket access token comparison is not required for access request from workspace pid: " + actualWorkspaceId + ".");
    } else if (!body.accessToken.equals(accessTokenBean.getToken())) {
      log.error("Invalid websocket access token provided by workspace pid: " + actualWorkspaceId +", rejecting and closing session.");

      // send rejection message then close the session.
      Message response = new Message(responseHeaders, new AccessResponseBody(false, 0));
      sendMessage(senderSession, response);

      senderSession.close();
      return;
    }

    // Ensure that the workspace id is not already claimed to be authorized.
    final int finalActualWorkspaceId = actualWorkspaceId;
    if (sessions.values().stream().anyMatch(ws -> ws.processId() == finalActualWorkspaceId)) {
      log.error("Workspace pid " + actualWorkspaceId + " is already authorized. Closing new session.");
      senderSession.close();
      return;
    }

    // Good and authorized.

    // Store new authorized workspace session with the workspace process id in sessions map.
    WorkspaceSession newWorkspaceSession = new WorkspaceSession(actualWorkspaceId);
    sessions.put(senderSession, newWorkspaceSession);

    // Reply to the requesting workspace with a successful access response.
    Message response = new Message(responseHeaders, new AccessResponseBody(true, this.sessions.size()));
    sendMessage(senderSession, response);

    log.debug("Sent successful access response.");

    // Broadcast a message to all other workspaces that the authorized workspaces count has changed.
    broadcastWorkspacesChanged(newWorkspaceSession);
  }

  /**
   * Send a message to all workspaces other than the one that caused the change that the count of authorized workspaces has changed.
   * Used whenever an authorized workspace is added or removed.
   */
  private void broadcastWorkspacesChanged(WorkspaceSession changedWorkspace) throws java.io.IOException {
    // changedWorkspace was either just added or removed. Inform the other workspaces about the new connected/authorized workspace count.

    Message message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, Audience.workspaces, "sidecar"),
        new WorkspacesChangedBody(this.sessions.size())
    );

    broadcast(message, changedWorkspace);
  }

  /**
   * Broadcast this message to all other authorized workspaces. Is what we do with all audience=="workspaces" messages,
   * either originating from other workspaces or perhaps from sidecar itself.
   *
   * Skips sending the message to the distinguished "sender" workspace.
   * */
  private void broadcast(Message message, WorkspaceSession sender) throws java.io.IOException {
    if (message.getHeaders().audience != Audience.workspaces) {
      log.error("Message id " + message.getId() + " is not a workspaces message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-workspaces message to workspaces.");
    }

    String jsonMessage = mapper.writeValueAsString(message);
    log.debug("Broadcasting " + jsonMessage.length() + " char message, id " + message.getId() + " from workspace: " + sender.processId());

    sessions.entrySet().stream()
        .filter(pair -> pair.getValue().processId() != sender.processId() && pair.getKey().isOpen())
        .forEach(pair -> {
          log.debug("Broadcasting message " + message.getId() + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /** Send a directed message from sidecar to a specific (possibly unauthorized) websocket session. */
  private void sendMessage(Session recipient, Message message) throws java.io.IOException {
    String jsonMessage = mapper.writeValueAsString(message);
    log.info("Sending " + jsonMessage.length() + " char message, id " + message.getId() + " to workspace: " + recipient.getId());
    recipient.getAsyncRemote().sendText(jsonMessage);
  }
}
