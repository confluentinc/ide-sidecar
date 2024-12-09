package io.confluent.idesidecar.restapi.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


@QuarkusTest
public class WorkspaceProcessIdFilterTest {

  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @Test
  public void testFilterAddsWorkspacePIDs() {
    // Given
    MockWorkspaceProcess[] mockWorkspaceProcesses = new MockWorkspaceProcess[]{
        new MockWorkspaceProcess(),
        new MockWorkspaceProcess(),
        new MockWorkspaceProcess()
    };

    // Reset the handshaking process from any prior run tests.
    setAuthToken(null);

    // Handshake as the first process to get the access token, just as expected
    // in real life.
    String expectedTokenValue =
        RestAssured.given()
            .when()
            .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
                mockWorkspaceProcesses[0].pid_string)
            .get("/gateway/v1/handshake")
            .then()
            .assertThat()
            .statusCode(200)
            .extract()
            .jsonPath()
            .getString("auth_secret");

    // Should now know of mockWorkspaceProcesses[0]'s pid.
    var knownWorkspacePids = getKnownWorkspacePidsFromBean();
    assertTrue(knownWorkspacePids.contains(Long.valueOf(mockWorkspaceProcesses[0].pid_string)));
    // And the process is still alive, so ...
    assertTrue(knownWorkspacesBean.hasLivingWorkspaceClients());

    for (MockWorkspaceProcess mockWorkspaceProcess : mockWorkspaceProcesses) {
      // Make another lifecycle hit as each of the three mock vscode workspaces.
      RestAssured.given()
          .when()
          .header("Authorization", "Bearer " + expectedTokenValue)
          .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER,
              mockWorkspaceProcess.pid_string)
          .get("/gateway/v1/connections")
          .then()
          .assertThat()
          .statusCode(200);

      assertTrue(knownWorkspacePids.contains(
          Long.valueOf(mockWorkspaceProcess.pid_string)));
    }

    // They're all alive still so ...
    assertTrue(knownWorkspacesBean.hasLivingWorkspaceClients());

    // Now shut down each process, then check again and expect to have no more
    // known living clients. If the grim reaper task were running, it would then shut
    // down the sidecar.
    for (MockWorkspaceProcess mockWorkspaceProcess : mockWorkspaceProcesses) {
      mockWorkspaceProcess.stop();
    }
    assertFalse(knownWorkspacesBean.hasLivingWorkspaceClients());
  }

  @Test
  public void testFilterRejectsNonIntegerWorkspacePID() {
    // Reset the handshaking process from any prior run tests.
    setAuthToken(null);

    // Make a hit along with the header that our filter is looking for, but with a non-integer
    // value
    RestAssured.given()
        .when()
        .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER, "not-an-integer")
        .get("/gateway/v1/handshake")
        .then()
        .assertThat()
        .statusCode(400);
  }

  @Test
  public void testBadAccessTokenRejectsBeforeWorkspacePIDFilter() {
    // Set the access token to a known value.
    String expectedToken = "sdfsdfsdfsdf";
    setAuthToken(expectedToken);

    var rejectedPid = Long.valueOf(1234);

    // Hit connections route with our pid header, but with a bad access token.
    RestAssured.given()
        .when()
        .header("Authorization", "Bearer WrongTokenValue")
        .header(WorkspaceProcessIdFilter.WORKSPACE_PID_HEADER, rejectedPid.toString())
        .get("/gateway/v1/connections")
        .then()
        .assertThat()
        .statusCode(401);

    // Check that the pid was not added to the known workspaces, that the bad auth header
    // rejected the request first, by subtle inference. Add it directly, but expect the return
    // result to be true, indicating was just now newly added.
    assertTrue(knownWorkspacesBean.addWorkspacePID(rejectedPid));
  }

  /**
   * Reset the known workspaces bean to state before any hits have been made before each test.
   */
  @BeforeEach
  public void resetKnownWorkspacesBean() {
    try {
      Field allowNoWorkspacesField = knownWorkspacesBean.getClass()
          .getDeclaredField("allowNoWorkspaces");
      allowNoWorkspacesField.setAccessible(true);
      allowNoWorkspacesField.set(knownWorkspacesBean, true);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    getKnownWorkspacePidsFromBean().clear();

  }

  /**
   * Get at the knownWorkspacesBean's knownWorkspacePIDs field through cheating reflection (That
   * functionality not needed by any external business methods.)
   */
  private Set<Long> getKnownWorkspacePidsFromBean() {
    try {
      Field knownWorkspacePIDsField = knownWorkspacesBean.getClass()
          .getDeclaredField("knownWorkspacePIDs");
      knownWorkspacePIDsField.setAccessible(true);
      return (Set<Long>) knownWorkspacePIDsField.get(knownWorkspacesBean);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Explicitly set the access token value in the SidecarAccessTokenBean, consulted by its own
   * filter which runs prior to the workspace process id filter.
   *
   * @param value The value to set the access token to.
   */
  private void setAuthToken(String value) {
    try {
      Field tokenField = accessTokenBean.getClass().getDeclaredField("token");
      tokenField.setAccessible(true);
      tokenField.set(accessTokenBean, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
