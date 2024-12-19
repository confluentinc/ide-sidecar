package io.confluent.idesidecar.websocket.resources;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.websocket.resources.WebsocketEndpoint.WorkspaceWebsocketSession;
import jakarta.websocket.Session;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkspaceWebsocketSessionTest {

  @Test
  public void testMarkActiveOnlyWorksOnce() {
    // Given
    WorkspaceWebsocketSession session = new WorkspaceWebsocketSession(getMockSession());

    // Sessions should start inactive
    Assertions.assertFalse(session.isActive());

    // and become active when they have workspace pid assigned
    session.markActive(new WorkspacePid(1L));
    Assertions.assertTrue(session.isActive());

    // and will raise IllegalStateException if marked active again
    Assertions.assertThrows(IllegalStateException.class, () -> session.markActive(new WorkspacePid(2L)));
  }

  @Test
  public void testWorkspacePidString() {
    WorkspaceWebsocketSession session = new WorkspaceWebsocketSession(getMockSession());
    // inactive session should report "unknown" for its pid string
    Assertions.assertEquals("unknown", session.workspacePidString());
    // But should report the pid as string when active
    session.markActive(new WorkspacePid(1L));
    Assertions.assertEquals("1", session.workspacePidString());
  }


  private Session getMockSession() {
    return mock(Session.class);
  }

}
