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
import java.util.ArrayList;
import java.util.List;
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
  Map<String, List<String>> pendingMessages = new ConcurrentHashMap<>();

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

          // Process any queued messages that arrived while client was connecting
          List<String> queuedMessages = pendingMessages.remove(session.getId());
          if (queuedMessages != null && !queuedMessages.isEmpty()) {
            Log.infof("Processing %d queued messages for session %s", queuedMessages.size(), session.getId());
            for (String queuedMessage : queuedMessages) {
              client.sendToCCloud(queuedMessage);
            }
          }

          Log.infof("Opened a new session and added LSP client for session ID=%s",
              session.getId());
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
      // Queue message instead of closing session
      pendingMessages.computeIfAbsent(session.getId(), k -> new ArrayList<>()).add(message);
      Log.infof("Queued message for session %s (client not ready yet)", session.getId());
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

    // Clean up any pending messages
    pendingMessages.remove(session.getId());
  }
}
