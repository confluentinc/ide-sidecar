package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ServerEndpoint("/flsp")
@ApplicationScoped
public class FlinkLanguageServiceProxy {

  static final Integer MAX_RECONNECT_ATTEMPTS = 1;
  static final String LANGUAGE_SERVICE_URL_PATTERN = "wss://flinkpls.%s.%s.confluent.cloud/lsp";
  static final String CONNECTION_ID_PARAM_NAME = "connectionId";
  static final String REGION_PARAM_NAME = "region";
  static final String PROVIDER_PARAM_NAME = "provider";
  static final String ENVIRONMENT_ID_PARAM_NAME = "environmentId";
  static final String ORGANIZATION_ID_PARAM_NAME = "organizationId";

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  record AuthMessage (
      @JsonProperty(value = "Token") String token,
      @JsonProperty(value = "EnvironmentId") String environmentId,
      @JsonProperty(value = "OrganizationId") String organizationId
  ) {
  }

  record ProxyContext (
      String connectionId,
      String region,
      String provider,
      String environmentId,
      String organizationId
  ) {
    static ProxyContext from(Session session) {
      var paramMap = session.getRequestParameterMap();
      return new ProxyContext(
          paramMap.get(CONNECTION_ID_PARAM_NAME).get(0),
          paramMap.get(REGION_PARAM_NAME).get(0),
          paramMap.get(PROVIDER_PARAM_NAME).get(0),
          paramMap.get(ENVIRONMENT_ID_PARAM_NAME).get(0),
          paramMap.get(ORGANIZATION_ID_PARAM_NAME).get(0)
      );
    }

    String getConnectUrl() {
      // TODO: I guess this won't work for private networks, we'll need something more sophisticated
      return LANGUAGE_SERVICE_URL_PATTERN.formatted(
          region,
          provider
      );
    }
  }

  @ClientEndpoint
  class ProxyClient {
    Session remoteSession;
    Session localSession;
    ProxyContext context;
    AtomicInteger reconnectAttempts = new AtomicInteger(0);

    public ProxyClient(ProxyContext context, Session localSession) {
      this.context = context;
      this.localSession = localSession;
      try {
        var container = ContainerProvider.getWebSocketContainer();
        container.connectToServer(this, URI.create(context.getConnectUrl()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @OnOpen
    public void onOpen(Session remoteSession) {
      this.remoteSession = remoteSession;
      try {
        var connection = (CCloudConnectionState) connectionStateManager.getConnectionState(
            context.connectionId);
        this.remoteSession.getAsyncRemote().sendText(
            OBJECT_MAPPER.writeValueAsString(
                new AuthMessage(
                    connection.getOauthContext().getDataPlaneToken().token(),
                    context.environmentId,
                    context.organizationId
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
    public void onClose(Session session) throws IOException {
      // Increase number of reconnect attempts and close the session if the maximum number of
      // reconnect attempts has been reached
      if (reconnectAttempts.incrementAndGet() > MAX_RECONNECT_ATTEMPTS) {
        Log.errorf("Max reconnect attempts reached. Closing session.");
        localSession.close();
        return;
      }

      Log.infof(
          "Reconnecting to CCloud Language Service (Attempt %d/%d).",
          reconnectAttempts.get(),
          MAX_RECONNECT_ATTEMPTS
      );
      this.remoteSession = null;
      try {
        var container = ContainerProvider.getWebSocketContainer();
        container.connectToServer(this, URI.create(context.getConnectUrl()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public Future<Void> sendToCCloud(String message) {
      return this.remoteSession.getAsyncRemote().sendText(message);
    }

    public void close() {
      try {
        remoteSession.close();
      } catch (IOException e) {
        Log.error("Could not close WebSockets session to CCloud Language Service.", e);
      }
    }
  }

  @Inject
  ConnectionStateManager connectionStateManager;

  Map<String, ProxyClient> proxyClients = new ConcurrentHashMap<>();

  @OnOpen
  public void onOpen(Session session) throws IOException {
    var context = ProxyContext.from(session);
    if (proxyClients.containsKey(session.getId())) {
      session.close(
          new CloseReason(CloseCodes.CANNOT_ACCEPT, "Session ID already exists.")
      );
      return;
    }
    try {
      var connection = connectionStateManager.getConnectionState(context.connectionId);
      if (connection instanceof CCloudConnectionState cCloudConnectionState) {
        if (cCloudConnectionState.getOauthContext() == null
            || cCloudConnectionState.getOauthContext().hasReachedEndOfLifetime()
            || cCloudConnectionState.getOauthContext().getDataPlaneToken() == null) {
          session.close(
              new CloseReason(
                  CloseCodes.CANNOT_ACCEPT,
                  "Connection with ID=%s does not have a data plane token.".formatted(
                      context.connectionId)
              )
          );
        } else {
          var client = new ProxyClient(context, session);
          proxyClients.put(session.getId(), client);
          Log.infof("Added LSP client for session ID=%s", session.getId());
        }
      } else {
        session.close(
            new CloseReason(
                CloseCodes.CANNOT_ACCEPT,
                "Connection with ID=%s is not of type CCLOUD.".formatted(context.connectionId)
            )
        );
      }
    } catch (ConnectionNotFoundException e) {
      session.close(
          new CloseReason(
              CloseCodes.CANNOT_ACCEPT,
              "Could not find connection with ID=%s.".formatted(context.connectionId)
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
    }
  }
}
