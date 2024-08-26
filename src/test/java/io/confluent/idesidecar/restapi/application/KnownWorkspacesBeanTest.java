package io.confluent.idesidecar.restapi.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.idesidecar.restapi.testutil.MockWorkspaceProcess;
import jakarta.inject.Provider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class KnownWorkspacesBeanTest {

  @Test
  void testAddWorkspacePID() {
    var knownWorkspacesBean = new KnownWorkspacesBean();

    // Add a workspace PID
    var workspacePid = 123L;
    var newlyAdded = knownWorkspacesBean.addWorkspacePID(workspacePid);
    assertTrue(newlyAdded);

    // Add the same workspace PID again
    newlyAdded = knownWorkspacesBean.addWorkspacePID(workspacePid);
    assertFalse(newlyAdded);

    // Add a different workspace PID
    workspacePid = 456L;
    newlyAdded = knownWorkspacesBean.addWorkspacePID(workspacePid);
    assertTrue(newlyAdded);

    // Add the same workspace PID again
    newlyAdded = knownWorkspacesBean.addWorkspacePID(workspacePid);
    assertFalse(newlyAdded);
  }

  @Test
  void testHasLivingWorkspaceClients() {
    var knownWorkspacesBean = new KnownWorkspacesBean();

    // No workspace PIDs added yet, but we should report true
    // so that we can have a grace period before the first
    // workspace PID is added.

    assertTrue(knownWorkspacesBean.hasLivingWorkspaceClients());

    // Spawn a short-lived process as if was a workspace, add its PID, and then
    // check that we still have living workspace clients.
    try {
      var mockWorkspaceProcess = new MockWorkspaceProcess();

      knownWorkspacesBean.addWorkspacePID(mockWorkspaceProcess.pid);
      // Process is still alive, so we should report true.
      assertTrue(knownWorkspacesBean.hasLivingWorkspaceClients());

      // Kill the short-lived "workspace" process.
      mockWorkspaceProcess.stop();
    } catch (Exception e) {
      fail(e);
    }

    // Process is no longer alive, and we had first workspace process id reported,
    // so we should now report false.
    assertFalse(knownWorkspacesBean.hasLivingWorkspaceClients());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testPossiblyShutdownIfNoReasonToExist(boolean enabled) {
    var knownWorkspacesBean = new KnownWorkspacesBean();

    // Maybe let possiblyShutdownIfNoReasonToExist() call shutdown
    knownWorkspacesBean.grimReaperEnabled = new BooleanProvider(enabled);

    // Override the shutdown function being called away from Quarkus.asyncExit().
    var mockShutdownFunction = new MockShutdownable();
    knownWorkspacesBean.shutdownSystemFunction = mockShutdownFunction::shutdown;

    // First call to possiblyShutdownIfNoReasonToExist() should never call shutdown -- no
    // workspace PIDs added yet (if enabled, otherwise it should never call shutdown).
    knownWorkspacesBean.possiblyShutdownIfNoReasonToExist();
    assertFalse(mockShutdownFunction.wasCalled);

    try {
      // Spawn a short-lived process as if was a workspace, add its PID, and then
      // check again. We still should not call shutdown yet.
      var mockWorkspaceProcess = new MockWorkspaceProcess();

      knownWorkspacesBean.addWorkspacePID(mockWorkspaceProcess.pid);

      // Should never call shutdown, as we have a workspace process alive (or was not enabled).
      knownWorkspacesBean.possiblyShutdownIfNoReasonToExist();
      assertFalse(mockShutdownFunction.wasCalled);

      // Kill the short-lived "workspace" process.
      mockWorkspaceProcess.stop();

      // If we were enabled, we should call shutdown, as the workspace process is no longer alive.
      knownWorkspacesBean.possiblyShutdownIfNoReasonToExist();
      assertEquals(enabled, mockShutdownFunction.wasCalled);
    } catch (Exception e) {
      fail(e);
    }
  }

  private static class MockShutdownable {
    boolean wasCalled = false;

    // Implements functional interface ShutdownSystemFunction
    public void shutdown() {
      wasCalled = true;
    }
  }

  /**
   * Used to stub out KnownWorkspacesBean.grimReaperEnabled
   */
  private static class BooleanProvider implements Provider<Boolean> {

    boolean value;

    public BooleanProvider(boolean value) {
      this.value = value;
    }
    @Override
    public Boolean get() {
      return value;
    }
  }
}
