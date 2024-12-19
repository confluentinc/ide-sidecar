package io.confluent.idesidecar.websocket.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.websocket.resources.WebsocketEndpoint.WorkspaceWebsocketSession;
import jakarta.websocket.Session;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkspaceWebsocketSessionTest {

  @Test
  public void testActiveInactiveBehavior() {
    // inactive / not yet pid known session
    WorkspaceWebsocketSession session = new WorkspaceWebsocketSession(getMockSession(), null);
    Assertions.assertFalse(session.isActive());
    // inactive session should report "unknown" for its pid string
    assertEquals("unknown", session.workspacePidString());

    // But should report the pid as string when active
    session = new WorkspaceWebsocketSession(getMockSession(), new WorkspacePid(1L));
    assertTrue(session.isActive());
    assertEquals("1", session.workspacePidString());
  }

  private Session getMockSession() {
    return mock(Session.class);
  }

}
