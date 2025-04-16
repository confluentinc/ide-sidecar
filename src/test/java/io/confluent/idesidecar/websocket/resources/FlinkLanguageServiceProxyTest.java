package io.confluent.idesidecar.websocket.resources;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.auth.Token;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.quarkus.test.InjectMock;
import jakarta.websocket.CloseReason;
import jakarta.websocket.CloseReason.CloseCodes;
import jakarta.websocket.Session;
import java.io.IOException;
import java.net.URI;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.websocket.ClientEndpoint;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.OnClose;
import jakarta.websocket.OnMessage;
import jakarta.websocket.server.ServerEndpoint;
import java.time.Instant;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
public class FlinkLanguageServiceProxyTest {

  @TestHTTPResource("/flsp?connectionId=ccloud-flink&region=us-east1&provider=gcp&environmentId=env-id&organizationId=org-id")
  URI uri;

  @InjectMock
  ConnectionStateManager connectionStateManager;

  static LinkedBlockingDeque<String> messages = new LinkedBlockingDeque<>();
  static AtomicReference<CloseReason> closeReason = new AtomicReference<>(null);

  static final String CLIENT_RPC_REQUEST = """
      {
        "jsonrpc": "2.0",
        "id": "0",
        "method": "initialize",
        "params": {
          "capabilities": {
            "textDocument": {
              "moniker": {},
              "synchronization": {
                "dynamicRegistration": true,
                "willSave": false,
                "didSave": false,
                "willSaveWaitUntil": false
              },
              "completion": {
                "dynamicRegistration": true,
                "completionItem": {
                  "snippetSupport": false,
                  "commitCharactersSupport": true,
                  "documentationFormat": ["plaintext", "markdown"],
                  "deprecatedSupport": false,
                  "preselectSupport": false,
                  "insertReplaceSupport": false
                },
                "contextSupport": false
              },
              "declaration": {
                "dynamicRegistration": true,
                "linkSupport": true
              },
              "implementation": {
                "dynamicRegistration": true,
                "linkSupport": true
              }
            },
            "workspace": {
              "didChangeConfiguration": {
                "dynamicRegistration": true
              }
            }
          },
          "initializationOptions": null,
          "processId": null,
          "rootUri": null,
          "workspaceFolders": null
        }
      }
      """;

  static final String SERVER_RPC_RESPONSE = """
      {
        "id": "0",
        "result": {
          "capabilities": {
            "textDocumentSync": {
              "openClose": true,
              "change": 1
            },
            "completionProvider": {
            }
          }
        },
        "jsonrpc": "2.0"
      }
      """;

  static final String AUTH_MESSAGE =
      "{\"Token\":\"data-plane-token\",\"EnvironmentId\":\"env-id\",\"OrganizationId\":\"org-id\"}";

  @BeforeEach
  void setup() {
    messages = new LinkedBlockingDeque<>();
    closeReason.set(null);

    var oAuthContext = Mockito.spy(CCloudOAuthContext.class);
    Mockito
        .when(oAuthContext.getDataPlaneToken())
        .thenReturn(new Token("data-plane-token", Instant.now()));
    var mockedConnection = Mockito.spy(CCloudConnectionState.class);
    Mockito.when(mockedConnection.getOauthContext()).thenReturn(oAuthContext);
    Mockito
        .when(connectionStateManager.getConnectionState("ccloud-flink"))
        .thenReturn(mockedConnection);
  }

  @Test
  public void testSendingJsonRpcCall() throws Exception {
    try (var session = ContainerProvider.getWebSocketContainer().connectToServer(TestClient.class, uri)) {
      // Send an example valid message and check that the session will not be closed.
      session.getAsyncRemote().sendText(CLIENT_RPC_REQUEST);
      Assertions.assertEquals(SERVER_RPC_RESPONSE, messages.poll(10, TimeUnit.SECONDS));
      Assertions.assertNull(closeReason.get());
    }
  }

  @Test
  public void testSendingInvalidMessage() throws Exception {
    var session = ContainerProvider.getWebSocketContainer().connectToServer(TestClient.class, uri);
    // Send a first invalid message
    session.getAsyncRemote().sendText("This is an invalid message.").get();
    // Allow the proxy to reconnect to remote server, as the session should have been closed
    Thread.sleep(3_000);
    // The session between the client and the proxy should not yet been closed
    Assertions.assertNull(closeReason.get());
    Assertions.assertTrue(session.isOpen());
    // Send a second invalid message
    session.getAsyncRemote().sendText("This is another invalid message").get();
    // Allow the proxy to close the session
    Thread.sleep(3_000);
    // Verify that the session has been closed
    Assertions.assertNotNull(closeReason.get());
    Assertions.assertFalse(session.isOpen());
    // Check the close code and reason
    Assertions.assertEquals(CloseCodes.GOING_AWAY, closeReason.get().getCloseCode());
    Assertions.assertEquals(
        "Max reconnect attempts reached. Lost connection to the remote server.",
        closeReason.get().getReasonPhrase()
    );
  }

  @Test
  public void testOpeningProxyForUnauthenticatedConnection() throws Exception {
    // Create a connection without a valid OAuth context
    var mockedConnection = Mockito.spy(CCloudConnectionState.class);
    Mockito
        .when(connectionStateManager.getConnectionState("ccloud-flink"))
        .thenReturn(mockedConnection);

    var session = ContainerProvider.getWebSocketContainer().connectToServer(TestClient.class, uri);
    // Allow the proxy to close the session
    Thread.sleep(3_000);
    // Verify that the session has been closed
    Assertions.assertNotNull(closeReason.get());
    Assertions.assertFalse(session.isOpen());
    // Check the close code and reason
    Assertions.assertEquals(CloseCodes.CANNOT_ACCEPT, closeReason.get().getCloseCode());
    Assertions.assertEquals(
        "Connection with ID=ccloud-flink does not have a data plane token.",
        closeReason.get().getReasonPhrase()
    );
  }

  @Test
  public void testOpeningProxyForConnectionWithExpiredAuthContext() throws Exception {
    // Create a connection without a valid OAuth context
    var authContext = Mockito.spy(CCloudOAuthContext.class);
    Mockito.when(authContext.hasReachedEndOfLifetime()).thenReturn(true);
    var mockedConnection = Mockito.spy(CCloudConnectionState.class);
    Mockito.when(mockedConnection.getOauthContext()).thenReturn(authContext);
    Mockito
        .when(connectionStateManager.getConnectionState("ccloud-flink"))
        .thenReturn(mockedConnection);

    var session = ContainerProvider.getWebSocketContainer().connectToServer(TestClient.class, uri);
    // Allow the proxy to close the session
    Thread.sleep(3_000);
    // Verify that the session has been closed
    Assertions.assertNotNull(closeReason.get());
    Assertions.assertFalse(session.isOpen());
    // Check the close code and reason
    Assertions.assertEquals(CloseCodes.CANNOT_ACCEPT, closeReason.get().getCloseCode());
    Assertions.assertEquals(
        "Connection with ID=ccloud-flink does not have a data plane token.",
        closeReason.get().getReasonPhrase()
    );
  }

  @Test
  public void testOpeningProxyForConnectionWithoutDataPlaneToken() throws Exception {
    // Create a connection without a valid OAuth context
    var authContext = Mockito.spy(CCloudOAuthContext.class);
    Mockito.when(authContext.getDataPlaneToken()).thenReturn(null);
    var mockedConnection = Mockito.spy(CCloudConnectionState.class);
    Mockito.when(mockedConnection.getOauthContext()).thenReturn(authContext);
    Mockito
        .when(connectionStateManager.getConnectionState("ccloud-flink"))
        .thenReturn(mockedConnection);

    var session = ContainerProvider.getWebSocketContainer().connectToServer(TestClient.class, uri);
    // Allow the proxy to close the session
    Thread.sleep(3_000);
    // Verify that the session has been closed
    Assertions.assertNotNull(closeReason.get());
    Assertions.assertFalse(session.isOpen());
    // Check the close code and reason
    Assertions.assertEquals(CloseCodes.CANNOT_ACCEPT, closeReason.get().getCloseCode());
    Assertions.assertEquals(
        "Connection with ID=ccloud-flink does not have a data plane token.",
        closeReason.get().getReasonPhrase()
    );
  }

  @ClientEndpoint
  public static class TestClient {
    @OnMessage
    void handleReceivedMessage(String msg) {
      messages.add(msg);
    }

    @OnClose
    public void close(Session session, CloseReason closeReason) {
      FlinkLanguageServiceProxyTest.closeReason.set(closeReason);
    }
  }

  @ServerEndpoint("/fls-mock")
  public static class MockedCCloudLanguageService {
    boolean clientHasAuthenticated = false;

    @OnMessage
    public void onMessage(String message, Session session) throws IOException {
      // The first message sent by the client should be the auth message
      // The following messages can be JSON RPC calls
      if (message.equals(AUTH_MESSAGE)) {
        clientHasAuthenticated = true;
      } else if (!clientHasAuthenticated) {
        session.close(
            new CloseReason(
                CloseReason.CloseCodes.CANNOT_ACCEPT,
                "Client has not authenticated yet."
            )
        );
      } else if (message.equals(CLIENT_RPC_REQUEST)) {
        session.getAsyncRemote().sendText(SERVER_RPC_RESPONSE);
      } else {
        session.close(
            new CloseReason(
                CloseReason.CloseCodes.CANNOT_ACCEPT,
                "Unknown message received."
            )
        );
      }
    }
  }
}
