package io.confluent.idesidecar.websocket.proxy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.websocket.exceptions.ProxyConnectionFailedException;
import io.confluent.idesidecar.websocket.messages.FlinkLanguageServiceAuthMessage;
import io.quarkus.logging.Log;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
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

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
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
    // Open connection to CCloud Flink Language Service
    try {
      var container = ContainerProvider.getWebSocketContainer();
      container.connectToServer(this, URI.create(context.getConnectUrl()));
    } catch (Exception e) {
      throw new ProxyConnectionFailedException(e);
    }
  }

  @OnOpen
  public synchronized void onOpen(Session remoteSession) {
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

  @OnMessage
  public synchronized void onMessage(String message) {
    localSession.getAsyncRemote().sendText(message);
  }

  @OnClose
  public synchronized void onClose(Session session, CloseReason closeReason) throws IOException {
    Log.infof("Closing session normally with status: %s and reason: %s.",
        closeReason.getCloseCode().toString(),
        closeReason.getReasonPhrase()
    );
    // Forward close reason from CCloud service to client
    localSession.close(closeReason);
  }

  public synchronized Future<Void> sendToCCloud(String message) {
    var processedMessage = message.replace(
        CCLOUD_DATA_PLANE_TOKEN_PLACEHOLDER,
        context.connection().getOauthContext().getDataPlaneToken().token()
    );
    return this.remoteSession.getAsyncRemote().sendText(processedMessage);
  }

  public void close() {
    try {
      if (remoteSession != null) {
        remoteSession.close(
            new CloseReason(
                CloseCodes.NORMAL_CLOSURE,
                "Closing session from FlinkLanguageServiceProxyClient."
            )
        );
      }
    } catch (IOException e) {
      Log.error("Could not close WebSockets session to CCloud Language Service.", e);
    }
  }
}
