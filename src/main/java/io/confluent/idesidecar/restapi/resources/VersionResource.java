package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.models.SidecarVersionResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Route which describes this build of the sidecar.
 * Needed so that extension can determine if it is out of sync with its expected sidecar build.
 */
@Path(VersionResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
public class VersionResource {

  static final String API_RESOURCE_PATH = "/gateway/v1/version";

  /* UNSET and VERSION patterned after how determined in ...application.Main */
  static final String UNSET_VERSION = "unset";

  static final String VERSION = ConfigProvider
      .getConfig()
      .getOptionalValue("quarkus.application.version", String.class)
      .orElse(UNSET_VERSION);

  @GET
  public SidecarVersionResponse version() {
    return new SidecarVersionResponse(VERSION);
  }
}
