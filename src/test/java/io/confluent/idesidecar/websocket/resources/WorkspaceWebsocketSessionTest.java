package io.confluent.idesidecar.websocket.resources;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.confluent.idesidecar.websocket.resources.WebsocketEndpoint.WorkspaceWebsocketSession;
import jakarta.websocket.Session;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkspaceWebsocketSessionTest {


  @Test
  public void testActiveInactiveBehavior() {
    // inactive / not yet pid known session
    Instant now = Instant.now();
    WorkspaceWebsocketSession session = new WorkspaceWebsocketSession(getMockSession(), null, now);
    Assertions.assertFalse(session.isActive());
    // inactive session should report "unknown" for its pid string
    Assertions.assertEquals("unknown", session.workspacePidString());

    // But should report the pid as string when promoted to an active session
    session = session.makeActive(new WorkspacePid(1L));
    Assertions.assertTrue(session.isActive());
    Assertions.assertEquals("1", session.workspacePidString());
  }


  private Session getMockSession() {
    return mock(Session.class);
  }

}
