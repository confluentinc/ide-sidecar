package io.confluent.idesidecar.websocket.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.quarkus.test.common.http.TestHTTPResource;
import jakarta.inject.Inject;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for unit tests that involve websocket connections.
 */
abstract class AbstractWebsocketTestBase implements WebsocketClients {

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  @Inject
  WebsocketEndpoint websocketEndpoint;

  @Inject
  ObjectMapper mapper;

  @TestHTTPResource("/ws")
  URI uri;

  protected void expectKnownWorkspacePids(WorkspacePid... pids) {
    WebsocketClients.expectKnownWorkspacePids(knownWorkspacesBean, pids);
  }

  @BeforeEach
  public void resetKnownWorkspacesBean() {
    WebsocketClients.resetKnownWorkspacesBean(knownWorkspacesBean);
  }

  @BeforeEach
  public void resetAccessTokenBean() {
    WebsocketClients.resetAccessTokenBean(accessTokenBean);
  }

  @BeforeEach
  public void resetCurrentWebsocketCount() {
    // empty out websocketEndpoint's sessions, in case any from prior test
    // linger, which may throw off behavior of the next test.
    websocketEndpoint.sessions.clear();
  }

  /**
   * Explicitly set the access token value in the SidecarAccessTokenBean, consulted by the filter.
   *
   * @param value The value to set the access token to.
   */
  void setAuthToken(String value) {
    WebsocketClients.setAuthToken(accessTokenBean, value);
  }

  /**
   * Connect a new mock workspace process to the websocket endpoint.
   *
   * @param sayHello Whether to send a HELLO message after connecting.
   * @return A ConnectedWorkspace instance representing the connection.
   */
  ConnectedWorkspace connectWorkspace(boolean sayHello) {
    return connectWorkspace(sayHello, sayHello);
  }

  /**
   * Connect a new mock workspace process to the websocket endpoint.
   *
   * @param sayHello                     Whether to send a HELLO message after connecting.
   * @param consumeInitialWorkspaceCount Whether to wait for the initial WORKSPACE_COUNT_CHANGED
   *                                     response to the HELLO. (Only meaningful when sayHello is
   *                                     true.)
   * @return A ConnectedWorkspace instance representing the connection.
   */
  ConnectedWorkspace connectWorkspace(boolean sayHello, boolean consumeInitialWorkspaceCount) {
    // Create a workspace process
    var mockWorkspaceProcess = new MockWorkspaceProcess();
    expectKnownWorkspacePids(mockWorkspaceProcess.pid);

    return connectWorkspace(
        mockWorkspaceProcess.pid,
        sayHello,
        consumeInitialWorkspaceCount
    );
  }

  /**
   * Connect the specified mock workspace process to the websocket endpoint.
   *
   * @param workspacePid                 The ID of the workspace process that is connecting.
   * @param sayHello                     Whether to send a HELLO message after connecting.
   * @param consumeInitialWorkspaceCount Whether to wait for the initial WORKSPACE_COUNT_CHANGED
   *                                     response to the HELLO. (Only meaningful when sayHello is
   *                                     true.)
   * @return A ConnectedWorkspace instance representing the connection.
   */
  ConnectedWorkspace connectWorkspace(
      WorkspacePid workspacePid,
      boolean sayHello,
      boolean consumeInitialWorkspaceCount
  ) {
    if (consumeInitialWorkspaceCount && !sayHello) {
      throw new IllegalArgumentException(
          "consumeInitialWorkspaceCount only makes sense when sayHello is true.");
    }

    // Make it smell as if the handshake has happened.
    var expectedTokenValue = "valid-token";
    setAuthToken(expectedTokenValue);
    // Make websocket connection request carry the right header value
    TestWebsocketClientConfigurator.setAccessToken(expectedTokenValue);

    return connectWorkspace(
        mapper,
        uri,
        "valid-token",
        workspacePid,
        sayHello,
        consumeInitialWorkspaceCount ? Duration.ofMillis(500) : null
    );
  }
}