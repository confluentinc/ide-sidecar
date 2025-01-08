package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.websocket.messages.HelloBody;
import io.confluent.idesidecar.websocket.messages.Message;
import io.confluent.idesidecar.websocket.messages.MessageHeaders;
import io.confluent.idesidecar.websocket.messages.MessageType;
import io.confluent.idesidecar.websocket.messages.ProtocolErrorBody;
import io.confluent.idesidecar.websocket.messages.WorkspacesChangedBody;
import io.quarkus.logging.Log;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import io.smallrye.common.constraint.Nullable;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;


/**
 * Websocket endpoint for "control plane" variety async messaging between sidecar and workspaces,
 * and/or workspace -> workspaces.
 */
@ServerEndpoint("/ws")
@Singleton
public class WebsocketEndpoint {

  /** How often to check for inactive sessions to purge. */
  @ConfigProperty(name = "ide-sidecar.websockets.purge-interval-seconds")
  Provider<Integer> purgeIntervalSeconds;

  /** How long to allow inactive sessions to linger before purging them. */
  @ConfigProperty(name = "ide-sidecar.websockets.initial-grace-seconds", defaultValue = "30")
  Provider<Integer> initialGraceSeconds;

  /**
   * Be sure to use the Quarkus-app-wide ObjectMapper instance, configured for extras like
   * java.time.Instant serialization.
   */
  @Inject
  ObjectMapper mapper;

  /** Typesafe wrapper for a websocket session id (string). */
  public static record SessionKey(String sessionId) {
    @Override
    public String toString() {
      return sessionId;
    }
  }

  /** Wrapper for a websocket session, with additional state tracking. */
  static class WorkspaceWebsocketSession {
    private final ObjectMapper mapper;
    private final SessionKey key;
    private final Session session;
    private final Instant createdAt;

    /** Will be null if session has not sent HELLO_WORKSPACE message carrying its process id. */
    @Nullable
    private final WorkspacePid workspacePid;

    WorkspaceWebsocketSession(ObjectMapper mapper, Session session, WorkspacePid workspacePid, Instant createdAt) {
      this.mapper = mapper;
      this.session = session;
      this.key = new SessionKey(session.getId());
      this.workspacePid = workspacePid;
      this.createdAt = createdAt;

    }

    /**
     * Return a new workspace session instance which knows its workspace pid, and thereby
     * is 'active'.
     */
    public WorkspaceWebsocketSession buildActive(WorkspacePid workspacePid) {
      return new WorkspaceWebsocketSession(this.mapper, this.session, workspacePid, this.createdAt);
    }

    /** The websocket session id in SessionKey wrapping, used as the key in the sessions map. */
    public SessionKey key() {
      return key;
    }

    public Session session() {
      return session;
    }

    public WorkspacePid workspacePid() {
      return workspacePid;
    }

    public String workspacePidString() {
      return workspacePid != null ? workspacePid.toString() : "unknown";
    }

    /**
     * Has this session sent a proper HELLO_WORKSPACE message and should be considered active
     * for sending and receiving other messages?
     */
    public boolean isActive() {
      return workspacePid != null;
    }

    public Instant createdAt() {
      return createdAt;
    }

    /**
     * Send the text message with the given message ID, and return a {@link Uni} that returns
     * the message ID when the message is sent.
     *
     * @param messageText
     * @param messageId
     * @return the message ID when the message is sent.
     */
    public Uni<String> sendAsync(String messageText, String messageId) {
      Log.debugf(
          "Sending broadcast message %s to workspace pid %d",
          messageId,
          workspacePid.id()
      );

      var future = CompletableFuture.supplyAsync(() -> {
        session.getAsyncRemote().sendText(messageText, result -> {
          if (result.isOK()) {
            Log.debugf(
                "Successfully sent broadcast message %s to workspace pid %d",
                messageId,
                workspacePid.id()
            );
          } else {
            var cause = result.getException();
            Log.errorf("Failed to send broadcast message to workspace: %s", cause.getMessage());
            throw new CompletionException(cause);
          }
        });
        return messageId;
      });
      return Uni.createFrom().completionStage(future);
    }
  }

  // Done with WorkspaceWebsocketSession definition, on to WorkspaceWebsocketSession proper.

  /**
   * Map of all workspace sessions (valid or not), keyed by the websocket
   * session id wrapped as SessionKey.
   */
  final Map<SessionKey, WorkspaceWebsocketSession> sessions = new ConcurrentHashMap<>();

  /**
   * Authority on the known workspaces in the system. Used to validate workspace ids.
   */
  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  /**
   * Broadcast a message originating from sidecar to all active connected workspaces.
   *
   * @param message the message to broadcast
   * @throws IOException              if there is an error serializing the message to JSON.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting,
   *                                  or if the Message is not JSONable.
   */
  public Multi<String> broadcast(Message message) throws IllegalArgumentException {
    return broadcast(message, WorkspaceWebsocketSession::isActive, "active");
  }

  /**
   * Broadcast a message originating from sidecar to connected workspaces that match the supplied
   * filter.
   *
   * @param message           the message to broadcast
   * @param filter            the filter to apply to the workspace sessions
   * @param sessionsAdjective the adjective to describe the sessions in the log message.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting,
   *                                  or if the Message is not JSONable.
   */
  public Multi<String> broadcast(
      Message message,
      Predicate<WorkspaceWebsocketSession> filter,
      String sessionsAdjective
  ) throws IllegalArgumentException {
    var activeWorkspaceSessions = sessions.values().stream()
                                          .filter(filter)
                                          .toList();

    if (activeWorkspaceSessions.isEmpty()) {
      Log.debugf("No %s workspaces to broadcast message to.", sessionsAdjective);
      return Multi.createFrom().empty();
    }

    final MessageHeaders headers = validateHeadersForSidecarBroadcast(message);

    String jsonMessage;
    try {
      // Serialize the message to JSON
      jsonMessage = mapper.writeValueAsString(message);
    } catch (IOException e) {
      Log.errorf("Failed to serialize message to JSON: %s", e.getMessage());
      throw new IllegalArgumentException("Failed to serialize message to JSON", e);
    }

    Log.debugf(
        "Broadcasting %d char message, id %s to all workspaces",
        jsonMessage.length(),
        headers.id()
    );

    return broadcast(jsonMessage, headers.id(), filter, sessionsAdjective);

  }

  /**
   * Broadcast a message originating from sidecar to connected workspaces that match the supplied
   * filter.
   *
   * @param jsonMessage       the message to broadcast
   * @param messageId         the ID of the message (used for logging)
   * @param filter            the filter to apply to the workspace sessions
   * @param sessionsAdjective the adjective to describe the sessions in the log message.
   * @throws IOException              if there is an error serializing the message to JSON.
   * @throws IllegalArgumentException if the headers of the message are not valid for broadcasting.
   * */
  public Multi<String> broadcast(
      String jsonMessage,
      String messageId,
      Predicate<WorkspaceWebsocketSession> filter,
      String sessionsAdjective
  ) {

    var activeWorkspaceSessions = sessions.values().stream()
                                          .filter(filter)
                                          .toList();

    if (activeWorkspaceSessions.isEmpty()) {
      Log.debugf("No %s workspaces to broadcast message to.", sessionsAdjective);
      return Multi.createFrom().empty();
    }

    var promises = activeWorkspaceSessions
        .stream()
        .map(session -> session.sendAsync(jsonMessage, messageId))
        .toList();
    return Multi
        .createFrom()
        .iterable(promises)
        .onItem()
        .transformToMultiAndMerge(Uni::toMulti);
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
    Log.infof("New websocket session opened, not yet considered active: %s", session.getId());

    // Request must have had a valid access token to pass through AccessTokenFilter,
    // so we can assume that the session is authorized.
    // The workspace process id will come to us in a subsequent HELLO_WORKSPACE
    // message, at which time we'll replace this mapping with one that carries the pid.
    sessions.put(
        new SessionKey(session.getId()),
        new WorkspaceWebsocketSession(this.mapper, session, null, Instant.now())
    );
  }

  /**
   * Handle an incoming message from a websocket session. Called by the websocket server
   * when a message is received.
   * @param messageString The message as a JSON string, which will be parsed as a
   *                      {@link io.confluent.idesidecar.websocket.messages.Message} object
   * @param senderSession The websocket session that sent the message.
   */
  @OnMessage
  public void onMessage(String messageString, Session senderSession) throws IOException {
    var workspaceSession = sessions.get(new SessionKey(senderSession.getId()));
    if (workspaceSession == null) {
      // Strange. Shouldn't ever happen unless onOpen grossly err'd. Close the session.
      sendErrorAndCloseSession(
          senderSession,
          null,
          "Received message from unknown session %s. Closing session.",
          senderSession.getId()
      );
      return;
    }

    if (!workspaceSession.isActive()) {
      // Only expect a hello message when in this state.
      // Defer to internal handler for that sort of message.
      handleHelloMessage(workspaceSession, messageString);
      return;
    }

    // Message is from an active workspace session.
    Message m = deserializeMessage(workspaceSession, messageString, null);

    // Validate message.header.originator corresponds to the authorized workspace process id.
    // (messages sent from workspaces to sidecar should have the workspace's process id as
    // the originator)
    if (!validateOriginator(workspaceSession, m, workspaceSession.workspacePid())) {
      return;
    }

    Log.debugf(
        "Received message %s of messageType %s from workspace %s",
        m.id(),
        m.messageType(),
        workspaceSession.workspacePid()
    );

    // At this time, all websocket messages received from workspaces are intended to
    // be proxied to all the other workspaces. Do so here.
    broadcast(
        messageString,
        m.id(),
        wws -> wws.session().isOpen() && wws.isActive() && !wws.workspacePid().equals(workspaceSession.workspacePid()),
        "other"
    );
  }

  /**
   * Handle the first message from a workspace session,
   * which should be a HELLO_WORKSPACE message.
   *
   * If it is a valid HELLO_WORKSPACE message, the workspace is authorized and added to the
   * sessions map, and the new workspace count is broadcast to all workspaces (inclusive).
   * Otherwise an error message is sent to the session and it is closed.
   *
   * @param workspaceSession the session that sent the message.
   * @param messageString the JSON string message.
   */
  private void handleHelloMessage(WorkspaceWebsocketSession workspaceSession, String messageString) {

    // deserializeMessage() will close the connection if the message is invalid and
    // not a HELLO_WORKSPACE message
    var message = deserializeMessage(workspaceSession, messageString, MessageType.WORKSPACE_HELLO);
    if (message == null) {
      Log.errorf(
          "handleHelloMessage: Invalid message from not-yet-active session %s. Closed session.",
          workspaceSession.key()
      );
      return;
    }

    // Message is valid and is a HELLO_WORKSPACE message with a HelloBody.
    // Promote the long in its body to a WorkspacePid object.
    WorkspacePid workspacePid = new WorkspacePid(((HelloBody) message.body()).workspaceId());

    // Ensure that the header origin value corresponds to the
    // workspace id in the body.
    if (!validateOriginator(workspaceSession, message, workspacePid)) {
      // validateOriginator logged error and closed.
      return;
    }

    // Cross-reference the workspace id against the known workspaces.

    if (!knownWorkspacesBean.isKnownWorkspace(workspacePid)) {
      sendErrorAndCloseSession(
          workspaceSession,
          message.headers().id(),
          "handleHelloMessage: Unknown workspace pid %s. Closing session.",
          workspacePid
      );
      return;
    }

    // Should be unique across all active workspaces in session map.
    // Check to see if any active workspace sessions with the same workspace pid.
    var withSamePid = sessions.values().stream()
        .filter(wws -> wws.isActive() && wws.workspacePid().equals(workspacePid))
        .toList();

    if (!withSamePid.isEmpty()) {
        sendErrorAndCloseSession(
            workspaceSession,
            message.headers().id(),
            "Workspace id %s already connected. Closing session.",
            workspacePid
        );
        return;
    }

    // All good! Upgrade the session in the map to an active workspace session.
    sessions.put(
        workspaceSession.key(),
        workspaceSession.buildActive(workspacePid)
    );
    Log.infof("Session %s HELLO as pid %s authorized and marked active.", workspaceSession.key(), workspacePid);

    // Announce to all  workspaces (inclusive) that the active workspace count has changed.
    broadcastWorkspacesChanged();
  }

  @OnError
  public void onError(Session session, Throwable throwable) throws IOException {
    // The session should be found in the map, assuming onOpen() is working properly.
    // (but be defensive anyway)
    var existingSession = sessions.get(new SessionKey(session.getId()));

    var pid = existingSession != null ? existingSession.workspacePidString() : "unknown";
    Log.errorf(
        "Websocket error for workspace pid %s, session id %s. Reason: %s. Closing session.",
        pid,
        session.getId(),
        throwable.getMessage()
    );

    // onClose() will fire and remove the session from the map.
    session.close();
  }

  @OnClose
  public void onClose(Session session) {
    var existing = sessions.remove(new SessionKey(session.getId()));
    var pid = existing != null ? existing.workspacePidString() : "unknown";
    Log.infof("Workspace websocket session closed, pid %s, session id %s", pid, session.getId());

    if (existing != null && existing.isActive()) {
      // was an active workspace session. Announce to all other workspaces the list has changed
      broadcastWorkspacesChanged();
    }
  }

  /**
   * Send a message to all workspaces that the count of authorized workspaces has changed.
   * Used whenever a workspace is added or removed.
   */
  private void broadcastWorkspacesChanged()  {
    // changedWorkspace was either just added or removed. Inform all workspaces about the
    // new connected/authorized workspace count.

    if (sessions.isEmpty()) {
      Log.info("No workspaces connected. Not broadcasting workspace count change message.");
      return;
    }

    var message = new Message(
        new MessageHeaders(MessageType.WORKSPACE_COUNT_CHANGED),
        new WorkspacesChangedBody(this.sessions.size())
    );

    Log.info("Broadcasting workspace count change message to all workspaces.");

    broadcast(message);
  }

  /** Has purgeInactiveSessions logged the first time? */
  boolean purgeLogged = false;
  /**
   * Purge any sessions that have lingered too long
   * in the initial state without saying hello (""not active" sessions).
   */
  @Scheduled(
      every = "${ide-sidecar.websockets.purge-interval-seconds}s",
      concurrentExecution = ConcurrentExecution.SKIP)
  public void purgeInactiveSessions() {
    var maxAllowedSeconds = initialGraceSeconds.get();

    // Only log the first time to not to be too spammy.
    if (!purgeLogged) {
      Log.infof("Checking for overdue inactive websocket sessions to purge every %ds,"
          + " max allowed initial connection duration %ds.", purgeIntervalSeconds.get(), maxAllowedSeconds);
      purgeLogged = true;
    }

    // Find all inactive sessions that have been inactive for too long.
    var now = Instant.now();

    var toPurge = sessions.values().stream()
        .filter(wws -> !wws.isActive() && Duration.between(wws.createdAt(), now).toSeconds() > maxAllowedSeconds)
        .collect(Collectors.toList());

    // Purge any identified sessions.
    if(!toPurge.isEmpty()) {
      Log.infof("Purging %d inactive session(s) that have lingered too long.", toPurge.size());
      toPurge.forEach(wws -> {
        sendErrorAndCloseSession(
              wws,
              null,
              "Inactive session %s lingered too long. Closing.",
              wws.key()
          );
      });
    }
  }

  private void sendErrorAndCloseSession(
      WorkspaceWebsocketSession workspaceSession,
      String originalMessageId,
      String message,
      Object...messageParams
  ) {
    sendErrorAndCloseSession(workspaceSession.session(), originalMessageId, message, messageParams);
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
    // Remove the session from session map if it exists. It really should exist.
    // otherwise onOpen is broken.
    if (sessions.remove(new SessionKey(session.getId())) == null) {
      Log.warnf("Session %s not found when trying to send error message", session.getId());
      // but continue anyway so that we'll always end with closing the session.
    }

    // Synchronously send the error message to the session
    var msg = message.formatted(messageParams);
    Log.error(msg);
    try {
      var errorMessage = new Message(
          new MessageHeaders(MessageType.PROTOCOL_ERROR),
          new ProtocolErrorBody(msg, originalMessageId)
      );
      // Do not convert to getBasicRemote(), for some reason breaks tests.
      session.getAsyncRemote()
             .sendText(mapper.writeValueAsString(errorMessage)).get();
    } catch (IOException | InterruptedException | ExecutionException e) {
      Log.errorf(
          e,
          "Unable to send error message to session %s: %s",
          session.getId(),
          e.getMessage()
      );
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    } finally {
      try {
        // And always close the session
        session.close();
      } catch (IOException e) {
        Log.errorf(
            e,
            "Unable to close session %s: %s",
            session.getId(),
            e.getMessage()
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

    if (!headers.originatedBySidecar()) {
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
   * @param workspaceSession the session that sent the message.
   * @param messageString    the JSON string message.
   * @param expectedType     the expected message type.
   * @return the deserialized message, or null if the message is invalid.
   */
  @VisibleForTesting
  Message deserializeMessage(
      @NotNull WorkspaceWebsocketSession workspaceSession,
      @NotNull String messageString,
      MessageType expectedType
  ) {
    Message m;
    try {
      m = mapper.readValue(messageString, Message.class);
    } catch (IOException e) {
      sendErrorAndCloseSession(
          workspaceSession,
          null,
          "Unparseable message from session %s. Discarding and closing.",
          workspaceSession.key()
      );
      return null;
    }

    // Optional check against message type and deserialized payload body
    if (expectedType != null) {
      var headers = m.headers();
      if (headers.messageType() != expectedType) {
        sendErrorAndCloseSession(
            workspaceSession,
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
              workspaceSession,
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
  private boolean validateOriginator(
      WorkspaceWebsocketSession session,
      Message message,
      WorkspacePid expectedWorkspacePid
  ) {
    Long claimedWorkspacePid;
    try {
      claimedWorkspacePid = Long.valueOf(message.headers().originator());
    } catch (NumberFormatException e) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "Invalid websocket message header originator value -- not an integer: %s. "
              + "Removing and closing session %s.",
          message.messageType(),
          session.key()
      );
      return false;
    }

    if (! claimedWorkspacePid.equals(expectedWorkspacePid.id())) {
      sendErrorAndCloseSession(
          session,
          message.headers().id(),
          "Workspace %s sent message with incorrect originator value: %d."
              + " Removing and closing session %s.",
          expectedWorkspacePid,
          claimedWorkspacePid,
          session.key()
      );
      return false;
    }

    return true;
  }
}
