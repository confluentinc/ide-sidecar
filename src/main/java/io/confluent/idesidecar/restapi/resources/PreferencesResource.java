package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.InvalidPreferencesException;
import io.confluent.idesidecar.restapi.models.Preferences;
import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

@Path(PreferencesResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class PreferencesResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/preferences";

  /**
   * A channel used to fire events whenever the preferences change.
   */
  @Inject
  Event<PreferencesSpec> preferencesChangeEvents;

  volatile Preferences preferences = new Preferences(
      new PreferencesSpec(null, null)
  );

  @GET
  public Preferences getPreferences() {
    return preferences;
  }

  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @APIResponses(
      value = {
          @APIResponse(
              responseCode = "200",
              description = "OK",
              content = {
                  @Content(
                      mediaType = "application/json",
                      schema = @Schema(implementation = Preferences.class)
                  )
              }
          ),
          @APIResponse(
              responseCode = "400",
              description = "Provided preferences are not valid",
              content = {
                  @Content(
                      mediaType = "application/json",
                      schema = @Schema(implementation = Failure.class)
                  )
              }
          )
      }
  )
  public Preferences updatePreferences(Preferences updatedPreferences)
      throws InvalidPreferencesException {
    updatedPreferences.spec().validate();
    preferences = new Preferences(updatedPreferences.spec());
    preferencesChangeEvents.fire(preferences.spec());
    return preferences;
  }
}
