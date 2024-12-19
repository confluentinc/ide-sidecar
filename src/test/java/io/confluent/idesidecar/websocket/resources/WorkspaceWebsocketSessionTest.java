package io.confluent.idesidecar.websocket.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    var mockSession = getMockSession();
    WorkspaceWebsocketSession session = new WorkspaceWebsocketSession(mockSession, null, now);
    Assertions.assertFalse(session.isActive());
    // inactive session should report "unknown" for its pid string
    assertEquals("unknown", session.workspacePidString());

    // But should report the pid as string when building an active version
    // with a workspace pid. (post-HELLO websocket message)
    session = session.buildActive(new WorkspacePid(1L));
    assertTrue(session.isActive());
    assertEquals("1", session.workspacePidString());
    // remains the same.
    assertEquals(now, session.createdAt());
    assertEquals(mockSession, session.session());
  }

  private Session getMockSession() {
    return mock(Session.class);
  }

}
