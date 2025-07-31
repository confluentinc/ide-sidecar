package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.websocket.proxy.FlinkLanguageServiceProxyClient;
import io.confluent.idesidecar.websocket.proxy.ProxyContext;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WebSocket endpoint for the Flink Language Service Proxy.
 */
@ServerEndpoint("/flsp")
@ApplicationScoped
public class FlinkLanguageServiceProxy {

  public static final String INITIAL_MESSAGE = "OK";

  @Inject
  ConnectionStateManager connectionStateManager;

  Map<String, FlinkLanguageServiceProxyClient> proxyClients = new ConcurrentHashMap<>();

  @OnOpen
  public void onOpen(Session session) throws IOException {
    var context = ProxyContext.from(session);
    if (proxyClients.containsKey(session.getId())) {
      Log.warnf("Failed to open a new session: Session ID=%s already exists.", session.getId());
      session.close(
          new CloseReason(CloseCodes.CANNOT_ACCEPT, "Session ID already exists.")
      );
      return;
    }
    try {
      var connection = connectionStateManager.getConnectionState(context.connectionId());
      if (connection instanceof CCloudConnectionState cCloudConnectionState) {
        if (cCloudConnectionState.getOauthContext() == null
            || cCloudConnectionState.getOauthContext().hasReachedEndOfLifetime()
            || cCloudConnectionState.getOauthContext().getDataPlaneToken() == null) {
          Log.warnf(
              "Failed to open a new session: Connection with ID=%s lacks valid data plane token.",
              context.connectionId()
          );
          session.close(
              new CloseReason(
                  CloseCodes.CANNOT_ACCEPT,
                  "Connection with ID=%s does not have a data plane token.".formatted(
                      context.connectionId())
              )
          );
        } else {
          var client = new FlinkLanguageServiceProxyClient(
              context.withConnection(cCloudConnectionState),
              session
          );
          client.connectToCCloud();
          proxyClients.put(session.getId(), client);
          Log.infof("Opened a new session and added LSP client for session ID=%s",
              session.getId());
          // Let the client know that the connection to the Language Service is established
          session.getAsyncRemote().sendText(INITIAL_MESSAGE);
        }
      } else {
        Log.warnf(
            "Failed to open a new session: Connection with ID=%s is not of type CCLOUD.",
            context.connectionId()
        );
        session.close(
            new CloseReason(
                CloseCodes.CANNOT_ACCEPT,
                "Connection with ID=%s is not of type CCLOUD.".formatted(context.connectionId())
            )
        );
      }
    } catch (ConnectionNotFoundException e) {
      Log.warnf(
          "Failed to open a new session: Connection with ID=%s not found.",
          context.connectionId()
      );
      session.close(
          new CloseReason(
              CloseCodes.CANNOT_ACCEPT,
              "Could not find connection with ID=%s.".formatted(context.connectionId())
          )
      );
    }
  }

  @OnMessage
  public void onMessage(String message, Session session) throws IOException {
    var client = proxyClients.get(session.getId());
    if (client != null) {
      client.sendToCCloud(message);
    } else {
      Log.warnf(
          "Closing session when processing message because no client exists for session ID=%s",
          session.getId()
      );
      session.close(
          new CloseReason(
              CloseCodes.CANNOT_ACCEPT,
              "No client exists for the given session."
          )
      );
    }
  }

  @OnClose
  public void onClose(Session session) {
    var client = proxyClients.get(session.getId());
    if (client != null) {
      // Close WebSockets session to CCloud Language Service
      client.close();
      // Remove client
      proxyClients.remove(session.getId());
      Log.infof("Removed LSP client for session ID=%s", session.getId());
    } else {
      Log.infof("Couldn't find client for session ID=%s, nothing to remove.", session.getId());
    }
  }
}
