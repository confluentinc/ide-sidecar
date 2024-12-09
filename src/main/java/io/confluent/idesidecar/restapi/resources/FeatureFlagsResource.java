package io.confluent.idesidecar.restapi.resources;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.idesidecar.restapi.exceptions.ExceptionMappers;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.FeatureFlagFailureException;
import io.confluent.idesidecar.restapi.exceptions.FlagNotFoundException;
import io.confluent.idesidecar.restapi.featureflags.FeatureFlags;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * API endpoints related to feature flags.
 *
 * <p>Currently, the only endpoint is to evaluate a specific feature flag as defined in
 * LaunchDarkly projects for the currently-defined <i>context</i>, which includes a randomly
 * generated {@link java.util.UUID} (constant throughout the life of this process) and, when
 * authenticated into Confluent Cloud, the ID of the authenticated CCloud user.
 */
@Path(FeatureFlagsResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Feature Flags", description = "Feature flags")
public class FeatureFlagsResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/feature-flags";

  @Inject
  FeatureFlags featureFlags;

  /**
   * Get the current evaluated value of the feature flag with the given identifier.
   *
   * @param id the identifier of the flag
   * @return the value for the flag as most recently evaluated
   * @throws FlagNotFoundException if there is no flag with the given identifier, resulting in a
   *                               {@link ExceptionMappers#mapFlagNotFoundException 404 Not Found
   *                               error}.
   */
  @GET
  @Path("{id}/value")
  @APIResponses(value = {
      @APIResponse(
          responseCode = "200",
          description = "OK",
          content = {
              @Content(mediaType = "application/json",
                  schema = @Schema(implementation = JsonNode.class))
          }),
      @APIResponse(
          responseCode = "401",
          description = "Not Authorized",
          content = {
              @Content(mediaType = "application/json",
                  schema = @Schema(implementation = Failure.class))
          }),
      @APIResponse(
          responseCode = "404",
          description = "Feature flag not found",
          content = {
              @Content(mediaType = "application/json",
                  schema = @Schema(implementation = Failure.class))
          }),
      @APIResponse(
          responseCode = "500",
          description = "Internal error parsing feature flags",
          content = {
              @Content(mediaType = "application/json",
                  schema = @Schema(implementation = Failure.class))
          }),
  })
  public JsonNode evaluateFlag(String id)
      throws FlagNotFoundException, FeatureFlagFailureException {
    Log.debugf("Evaluating feature flag '%s'", id);
    return featureFlags
        .evaluateAsJson(id)
        .orElseThrow(() -> new FlagNotFoundException(id));
  }
}
