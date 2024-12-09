package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.models.SidecarVersionResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * Route which describes this build of the sidecar. Needed so that extension can determine if it is
 * out of sync with its expected sidecar build.
 */
@Path(VersionResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class VersionResource {

  static final String API_RESOURCE_PATH = "/gateway/v1/version";

  @Inject
  SidecarInfo sidecar;

  @GET
  public SidecarVersionResponse version() {
    return new SidecarVersionResponse(sidecar.version());
  }
}
