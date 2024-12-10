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


@ServerEndpoint("/pubsub")
@ApplicationScoped
public class WebsocketEndpoint {

  private static final Logger log = LoggerFactory.getLogger(WebsocketEndpoint.class);
  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @ConfigProperty(name = "ide-sidecar.access_token_filter.enabled", defaultValue = "true")
  Provider<Boolean> authorization_required;

  private final Map<Session, WorkspaceSession> sessions = new ConcurrentHashMap<Session, WorkspaceSession>();

  private final ObjectMapper mapper = new ObjectMapper();


  @OnMessage
  public void onMessage(String messageString, Session senderSession) throws java.io.IOException {
    WorkspaceSession workspaceSession = sessions.get(senderSession);
    if (workspaceSession == null) {
      // only message we expect from an unauthorized (not present in our map yet) session is an access request message.
      handleAccessRequestMessage(senderSession, messageString);
      return;
    }

    // Handle all other message types ...
    Message m = parseAndValidateMessage(messageString);

    if (m == null) {
      System.err.println("Invalid message from workspace: " + workspaceSession.processId() + ", discarding.");
      return;
    }

    MessageHeaders headers = m.getHeaders();

    System.out.println("Received " + headers.type + " message from workspace: " + workspaceSession.processId());

    if (headers.audience == Audience.workspaces)
    {
      int otherCount = sessions.size() - 1;
      if (otherCount == 0) {
        System.out.println("No other workspaces to broadcast message to.");
        return;
      } else {
        System.out.println("Broadcasting message to " + otherCount + " other workspaces");
        broadcast(m, workspaceSession);
      }

    } else if (headers.audience == Audience.sidecar) {
      // Message must be intended for sidecar
      // todo handle extension -> sidecar messages when we design some.
      System.err.println("Unexpected message audience: " + m.getHeaders().audience);
    } else {
      // We don't (yet) support or design for directed workspace -> workspace messages.
      System.err.println("Unhandled message audience: " + m.getHeaders().audience);
    }
  }

  @OnOpen
  public void onOpen(Session session) {
    // Don't store into sessions map until authorized, so do nothing of importance here.
    System.out.println("Session opened: " + session.getId());
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    // May or may not actually remove -- if had not yet been authorized, it won't be in the map.
    WorkspaceSession existingSession = sessions.remove(session);

    String logPrefix;
    if (existingSession != null) {
      logPrefix = "Websocket error for authorized workspace: " + existingSession.processId() + " - ";
    } else {
      logPrefix = "Websocket error for unauthorized session: " + session.getId() + " - ";
    }
    System.err.println(logPrefix + throwable.getMessage());
    System.err.println(logPrefix + "Session removed.");
  }

  @OnClose
  public void onClose(Session session) {
    WorkspaceSession existing = sessions.remove(session);
    System.out.println("Session closed: " + session.getId());

    if (existing != null) {
      // was a registered workspace session. Announce to all other workspaces that the list has changed.
      try {
        broadcastWorkspacesChanged(existing);
      } catch (java.io.IOException e) {
        System.err.println("Failed to broadcast workspace removed message: " + e.getMessage());
      }
    }
  }

  private Message parseAndValidateMessage(String message) {

    try {

      // deserialize the message using the mapper
      Message m = mapper.readValue(message, Message.class);

      MessageHeaders headers = m.getHeaders();

      // header.originator for messages recv'd by sidecar should always be a string'd integer
      if (!headers.originator.matches("\\d+")) {
        System.err.println("Invalid originator field");
        return null;
      }

      return m;
    } catch (Exception e) {
      System.err.println("Failed to parse message: " + e.getMessage());
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
    //

    Message message = parseAndValidateMessage(stringMessage);
    if (message == null) {
      System.err.println("Invalid message, expected an access request message but was not parseable.");
      senderSession.close();
      return;
    }

    if (!(message.getBody() instanceof AccessRequestBody)) {
      System.err.println("Expected AccessRequestBody as the payload, got " + message.getBody().getClass().getName() + " instead.");
      senderSession.close();
      return;
    }

    AccessRequestBody body = (AccessRequestBody) message.getBody();
    String accessToken = body.accessToken;

    MessageHeaders headers = message.getHeaders();

    // The originator will be the workspace's (process) id.
    int actualWorkspaceId = Integer.parseInt(headers.originator);

    System.out.println("Received authorization request from workspace: " + actualWorkspaceId + " with access token " + accessToken);;

    ResponseMessageHeaders responseHeaders =  new ResponseMessageHeaders(
        MessageType.ACCESS_RESPONSE,
        headers.id
    );
    
    if (!authorization_required.get()) {
      System.out.println("Authorization comparison is not required");
    } else if (!accessToken.equals(accessTokenBean.getToken())) {
      System.err.println("Invalid access token, rejecting.");

      Message response = new Message(responseHeaders, new AccessResponseBody(false, 0));
      sendMessage(senderSession, response);

      senderSession.close();
      return;
    }

    // Authorized.

    // Store new authorized workspace session with the process id in sessions map.
    WorkspaceSession newWorkspaceSession = new WorkspaceSession(actualWorkspaceId);
    sessions.put(senderSession, newWorkspaceSession);

    // Reply to the requesting workspace with a successful access response.
    Message response = new Message(responseHeaders, new AccessResponseBody(true, this.sessions.size()));
    sendMessage(senderSession, response);

    System.out.println("Sent successful access response.");

    // Broadcast a message to all other workspaces that the workspaces list has changed.
    broadcastWorkspacesChanged(newWorkspaceSession);
  }

  private void broadcastWorkspacesChanged(WorkspaceSession changedWorkspace) throws java.io.IOException {

    // changedWorkspace was either just added or removed. Tell all the other workspaces about the new size.
    // (Don't need to tell the changed workspace about itself --- it was told the current count in its access response message.)

    WorkspacesChangedBody body = new WorkspacesChangedBody(this.sessions.size());
    Message message = new Message(new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED, Audience.workspaces, "sidecar"), body);
    broadcast(message, changedWorkspace);
  }

  private void sendMessage(Session recipient, Message message) throws java.io.IOException {
    String jsonMessage = mapper.writeValueAsString(message);
    System.out.println("Sending " + jsonMessage.length() + " char message, id " + message.getId() + " to workspace: " + recipient.getId());
    recipient.getAsyncRemote().sendText(jsonMessage);
  }

  /**
   * Broadcast this message to all other authorized workspaces. Is what we do with all audience=="workspaces" messages,
   * either originating from other workspaces or perhaps from sidecar itself.
   *
   * Skips sending the message to the distinguished "sender" workspace.
   * */
  private void broadcast(Message message, WorkspaceSession sender) throws java.io.IOException {
    if (message.getHeaders().audience != Audience.workspaces) {
      System.out.println("Message id " + message.getId() + " is not a workspaces message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-workspaces message to workspaces.");
    }

    String jsonMessage = mapper.writeValueAsString(message);
    System.out.println("Broadcasting " + jsonMessage.length() + " char message, id " + message.getId() + " from workspace: " + sender.processId());

    sessions.entrySet().stream()
        .filter(pair -> pair.getValue().processId() != sender.processId() && pair.getKey().isOpen())
        .forEach(pair -> {
          System.out.println("Broadcasting message " + message.getId() + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }

  /** Broadcast a message originating from sidecar to all workspaces. */
  private void broadcast(Message message) throws java.io.IOException {
    if (message.getHeaders().audience != Audience.workspaces) {
      System.out.println("Message id " + message.getId() + " is not a workspaces message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-workspaces message to workspaces.");
    }

    if (! message.getHeaders().originator.equals("sidecar")) {
      System.out.println("Message id " + message.getId() + " is not a sidecar message, cannot broadcast.");
      throw new IllegalArgumentException("Attempted to broadcast a non-sidecar message to workspaces.");
    }

    if (sessions.size() == 0) {
      System.out.println("No workspaces to broadcast message to.");
      return;
    }

    String jsonMessage = mapper.writeValueAsString(message);
    System.out.println("Broadcasting " + jsonMessage.length() + " char message, id " + message.getId() + " to all workspaces");

    sessions.entrySet().stream()
        .filter(pair -> pair.getKey().isOpen())
        .forEach(pair -> {
          System.out.println("Broadcasting message " + message.getId() + " to workspace: " + pair.getValue().processId());
          pair.getKey().getAsyncRemote().sendText(jsonMessage);
        });
  }
}
