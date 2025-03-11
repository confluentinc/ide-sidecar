package io.confluent.idesidecar.restapi.filters;

import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean;
import io.confluent.idesidecar.restapi.application.KnownWorkspacesBean.WorkspacePid;
import io.quarkus.logging.Log;
import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;

/**
 * All sidecar requests should include the workspace process id in the x-workspace-process-id
 * header.  Take note of them so that we can eventually shut down the sidecar if there are no more
 * workspaces left.
 */
public class WorkspaceProcessIdFilter {

  public static final String WORKSPACE_PID_HEADER = "x-workspace-process-id";

  @Inject
  KnownWorkspacesBean knownWorkspacesBean;

  /**
   * Filter method that observes the x-workspace-process-id request header and adds the workspace
   * process id to the known set of workspaces.
   * <p>Runs at a lower priority than {@link AccessTokenFilter} to ensure that we do not honor
   * requests that do not have a valid access token. (Yes, lower number == lower priority, according
   * to @RouteFilter philosophy.)</p>
   **/
  @RouteFilter(90)
  public void filter(RoutingContext context) {
    String workspacePid = context.request().getHeader(WORKSPACE_PID_HEADER);
    if (workspacePid != null) {
      long workspacePidLong;
      try {
        workspacePidLong = Long.parseLong(workspacePid);
      } catch (NumberFormatException e) {
        Log.errorf("Invalid header %s workspace process id %s: cannot parse as long",
            WORKSPACE_PID_HEADER, workspacePid);
        context.fail(400);
        return;
      }

      boolean newlyAdded = knownWorkspacesBean.addWorkspacePid(new WorkspacePid(workspacePidLong));
      if (newlyAdded) {
        Log.infof("Added workspace process id %s to known workspaces", workspacePid);
      }
    }

    // Continue processing the request (if wasn't a non integer value). We treat the header as an
    // optional thing here, even though most all requests coming from sidecar should have it,
    // especially handshaking requests.

    context.next();
  }

}
