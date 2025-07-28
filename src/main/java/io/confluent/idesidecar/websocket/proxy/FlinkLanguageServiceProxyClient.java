package io.confluent.idesidecar.websocket.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.idesidecar.websocket.exceptions.ProxyConnectionFailedException;
import io.confluent.idesidecar.websocket.messages.FlinkLanguageServiceAuthMessage;
import io.quarkus.logging.Log;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Future;

/**
 * WebSocket client for connecting to the CCloud Flink Language Service.
 */
@ClientEndpoint
public class FlinkLanguageServiceProxyClient implements AutoCloseable {

  static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  static final String CCLOUD_DATA_PLANE_TOKEN_PLACEHOLDER = "{{ ccloud.data_plane_token }}";

  Session remoteSession;
  Session localSession;
  ProxyContext context;

  private FlinkLanguageServiceProxyClient() {}

  public FlinkLanguageServiceProxyClient(
      ProxyContext context,
      Session localSession
  ) {
    this.context = context;
    this.localSession = localSession;

    // log before attempting connect
    Log.infof("Attempting WebSocket connect to %s for local session %s",
              context.getConnectUrl(), localSession.getId());

    try {
      var container = ContainerProvider.getWebSocketContainer();
      container.connectToServer(this, URI.create(context.getConnectUrl()));
      // log after returning from connect
      Log.infof("connectToServer returned for %s", context.getConnectUrl());
    } catch (Exception e) {
      Log.warnf("Failed to connect to CCloud Flink Language Service at %s: %s",
                context.getConnectUrl(), e.getMessage(), e);
      throw new ProxyConnectionFailedException(e);
    }
  }

  @OnOpen
  public synchronized void onOpen(Session remoteSession) {
    // log when remote endpoint handshake completes
    Log.infof("CCloud WebSocket onOpen: remoteSession=%s localSession=%s",
              remoteSession.getId(), localSession.getId());

    this.remoteSession = remoteSession;
    try {
      // After opening the connection, we need to send the auth message to the Language Service
      this.remoteSession.getAsyncRemote().sendText(
          OBJECT_MAPPER.writeValueAsString(
              new FlinkLanguageServiceAuthMessage(
                  context.connection().getOauthContext().getDataPlaneToken().token(),
                  context.environmentId(),
                  context.organizationId()
              )
          )
      );
    } catch (Exception e) {
      Log.errorf("Failed to send initial auth message: %s", e.getMessage());
    }
  }

  @OnError
  public synchronized void onError(Session session, Throwable t) {
    Log.errorf("WebSocket error on CCloud session %s for local session %s: %s",
               session.getId(), localSession.getId(), t.getMessage(), t);
  }

  @OnMessage
  public synchronized void onMessage(String message) {
    localSession.getAsyncRemote().sendText(message);
  }

  @OnClose
  public synchronized void onClose(Session session, CloseReason closeReason) throws IOException {
    Log.infof("ClientEndpoint.onClose: remoteSession=%s code=%s reason=%s",
              session.getId(), closeReason.getCloseCode(), closeReason.getReasonPhrase());
    Log.infof("Forwarding close to localSession=%s", localSession.getId());
    localSession.close(closeReason);
    Log.infof("localSession %s closed in proxy client", localSession.getId());
  }

  public synchronized Future<Void> sendToCCloud(String message) {
    var processedMessage = message.replace(
        CCLOUD_DATA_PLANE_TOKEN_PLACEHOLDER,
        context.connection().getOauthContext().getDataPlaneToken().token()
    );
    return this.remoteSession.getAsyncRemote().sendText(processedMessage);
  }

  public void close() {
    Log.infof("close() invoked on proxy client: localSession=%s remoteSession=%s",
              localSession.getId(),
              remoteSession != null ? remoteSession.getId() : "null");
    try {
      if (remoteSession != null) {
        Log.infof("Closing remoteSession %s with NORMAL_CLOSURE", remoteSession.getId());
        remoteSession.close(
          new CloseReason(CloseCodes.NORMAL_CLOSURE,
                          "Closing session from FlinkLanguageServiceProxyClient.")
        );
        Log.infof("remoteSession %s closed", remoteSession.getId());
      }
    } catch (IOException e) {
      Log.error("Could not close WebSockets session to CCloud Language Service.", e);
    }
  }
}
