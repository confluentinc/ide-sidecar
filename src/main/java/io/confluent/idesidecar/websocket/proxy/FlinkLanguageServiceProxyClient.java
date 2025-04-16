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
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.ConfigProvider;

@ClientEndpoint
public class FlinkLanguageServiceProxyClient implements AutoCloseable {

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static final Integer MAX_RECONNECT_ATTEMPTS = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.flink-language-service-proxy.reconnect-attempts", Integer.class);
  static final String CCLOUD_DATA_PLANE_TOKEN_PLACEHOLDER = "{{ ccloud.data_plane_token }}";

  Session remoteSession;
  Session localSession;
  ProxyContext context;
  AtomicInteger reconnectAttempts = new AtomicInteger(0);

  private FlinkLanguageServiceProxyClient() {}

  public FlinkLanguageServiceProxyClient(
      ProxyContext context,
      Session localSession
  ) {
    this.context = context;
    this.localSession = localSession;
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
  public void onMessage(String message) {
    localSession.getAsyncRemote().sendText(message);
    // Connection seems to be healthy, let's reset the number of reconnect attempts
    reconnectAttempts.set(0);
  }

  @OnClose
  public synchronized void onClose(Session session, CloseReason closeReason) throws IOException {
    if (closeReason.getCloseCode().equals(CloseCodes.NORMAL_CLOSURE)) {
      Log.infof("Closing session normally.");
    } else if (reconnectAttempts.incrementAndGet() > MAX_RECONNECT_ATTEMPTS) {
      // Increase number of reconnect attempts and close the session if the maximum number of
      // reconnect attempts has been reached
      Log.errorf("Max reconnect attempts reached. Closing session.");
      localSession.close(
          new CloseReason(CloseCodes.CLOSED_ABNORMALLY, "Max reconnect attempts reached.")
      );
    } else {
      Log.infof("Reconnecting to CCloud Flink Language Service due to abnormal closure.");
      this.remoteSession = null;
      try {
        var container = ContainerProvider.getWebSocketContainer();
        container.connectToServer(this, URI.create(context.getConnectUrl()));
      } catch (Exception e) {
        throw new ProxyConnectionFailedException(e);
      }
    }
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
