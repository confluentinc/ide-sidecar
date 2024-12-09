package io.confluent.idesidecar.restapi.testutil;

/**
 * Class as test suite stand in for vs code workspace process. Needs to be an OS-level process that
 * will hang around long enough to be noticed by
 * {@link io.confluent.idesidecar.restapi.application.KnownWorkspacesBean}.
 */
public class MockWorkspaceProcess {

  // Want to spawn a real process that will hang around long enough even if breakpoint
  // stepping through tests. Up to a point.
  public final static String[] COMMAND = new String[]{
      "sleep", "900"
  };

  public final Long pid;
  public final String pid_string;
  private final Process process;

  public MockWorkspaceProcess() {
    try {
      ProcessBuilder pb = new ProcessBuilder(COMMAND);
      process = pb.start();
      pid = process.pid();
      pid_string = String.valueOf(pid);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start mock workspace process: " + e.getMessage());
    }
  }

  public void stop() {
    try {
      process.destroy();
      process.waitFor();
    } catch (Exception e) {
      throw new RuntimeException("Failed to stop mock workspace process: " + e.getMessage());
    }
  }
}
