package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.filters.WorkspaceProcessIdFilter;
import io.confluent.idesidecar.restapi.util.ShutdownSystemFunction;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Quarkus;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.ConcurrentExecution;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import java.util.Iterator;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Bean keeping track of the known VS Code workspace process ids that have interacted with
 * the sidecar.
 *
 * <p>Is populated via a filter that observes the {@code x-workspace-process-id} request header.</p>
 *
 *  <p>Has a periodic task that checks if the sidecar should remain alive based on the
 *  liveness of any known workspaces, once the first workspace has made contact via
 *  the handshake route and the
 *  {@link WorkspaceProcessIdFilter} request header
 *  filter. That task will shut down the server when no more vs code workspace process id
 *  clients remain alive.</p>
 */

@Singleton
public class KnownWorkspacesBean {

  /** Typesafe wrapper for a workspace process id. */
  public static record WorkspacePid(Long id) {
    @Override
    public String toString() {
      return String.valueOf(id);
    }
  }

  // What we call to shut down the system when we notice last vs code workspace
  // has exited. Default to Quarkus::asyncExit. Overrideable for test suite.
  ShutdownSystemFunction shutdownSystemFunction = Quarkus::asyncExit;

  // Config property to enable/disable scheduled task innards for
  // checking if the sidecar should remain alive.
  // (Must be a Provider to allow native compilations to still be able to override it
  // https://github.com/quarkusio/quarkus/issues/7607)
  @ConfigProperty(name = "ide-sidecar.grim-reaper.enabled", defaultValue = "true")
  Provider<Boolean> grimReaperEnabled;

  /**
   * The set of the vscode workspace process ids that have interacted with the sidecar
   * and are thought to be still alive.
   *
   * @see #hasLivingWorkspaceClients() for grooming.
   */
  Set<WorkspacePid> knownWorkspacePids = new ConcurrentHashSet<>();

  /**
   * Should {@link #hasLivingWorkspaceClients()} return true if there are no known workspaces?
   * Starts out true to allow an open-ended grace period prior to the first route
   * providing the x-workspace-process-id header having been hit. Once hit (and
   * first workspace pid added), then will be reset.
   */
  private boolean allowNoWorkspaces = true;

  /**
   * Adds a workspace process id to the set of known workspaces.
   * @param workspacePid the workspace id to add
   * @return true if the workspace id is new (not already known), false otherwise
   */
  public boolean addWorkspacePid(WorkspacePid workspacePid) {
    boolean wasNew = knownWorkspacePids.add(workspacePid);

    // First handshake has definitely happened by time the request filter calls addWorkspacePID().
    if (wasNew && allowNoWorkspaces) {
      allowNoWorkspaces = false;
    }

    return wasNew;
  }

  /**
   * Is this a known workspace id?
   */
  public boolean isKnownWorkspace(WorkspacePid workspaceId) {
    return knownWorkspacePids.contains(workspaceId);
  }

  /**
   * Check if the sidecar should still remain alive:
   *  - If no workspace has contacted us yet, (allowNoWorkspaces is true), return true.
   *  - Loops through all known workspace operating system-level process ids and removes any that
   *    are no longer alive.
   *  - If there are no more workspaces left, return false to indicate we should no
   *  longer be alive.
   *
   *  <p>Will be called by scheduled task checking to ensure that the sidecar should
   *  remain alive.</p>
   *
   * @return true if the sidecar should remain alive, false otherwise
   */
  public boolean hasLivingWorkspaceClients() {
    // Loop through all known workspaces and remove any that are no longer running.
    // If there are no more workspaces left, return false to indicate we should no
    // longer be alive.

    // Short circuit return true if we are still in the grace period.
    if (allowNoWorkspaces) {
      return true;
    }

    // Remove any workspace process ids that are no longer running.
    knownWorkspacePids.removeIf(workspacePid -> {
      var processHandle = ProcessHandle.of(workspacePid.id()).orElse(null);
      return processHandle == null || !processHandle.isAlive();
    });

    // We're happy if there's at least one workspace process still alive.
    return !knownWorkspacePids.isEmpty();
  }


  // Flag as to if within the scheduled task we have logged the first time. Keeps
  // us from being really spammy in the logs.
  private boolean hasLoggedInShutdownFirstTime = false;

  /**
   * Scheduled task that checks if the sidecar should remain alive, based on having
   * had at least one vs code workspace having used us but now none remain alive.
   * Will get called very early on in the lifecycle of the application, and then
   * every ${ide-sidecar.grim-reaper.interval-seconds}s thereafter.
   * <p>Will only run if grim reaper is enabled via configuration
   * knob `ide-sidecar.grim-reaper.enabled`.</p>
   */
  @Scheduled(
      every = "${ide-sidecar.grim-reaper.interval-seconds}s",
      concurrentExecution = ConcurrentExecution.SKIP)
  void possiblyShutdownIfNoReasonToExist() {

    if (grimReaperEnabled.get()) {
      if (!hasLoggedInShutdownFirstTime) {
        Log.info("Grim reaper: Will check if the sidecar should remain alive periodically.");
        hasLoggedInShutdownFirstTime = true;
      }

      if (!hasLivingWorkspaceClients()) {
        Log.info("Grim reaper: No living vs code workspaces remain. Shutting down.");
        shutdownSystemFunction.shutdown();
      }
    } else {
      if (!hasLoggedInShutdownFirstTime) {
        Log.info("Grim reaper: Disabled. Will not check if the sidecar should remain alive.");
        hasLoggedInShutdownFirstTime = true;
      }
    }
  }
}
