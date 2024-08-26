package io.confluent.idesidecar.restapi.resources;

import io.confluent.idesidecar.restapi.models.ApplyTemplateRequest;
import io.confluent.idesidecar.restapi.models.Template;
import io.confluent.idesidecar.restapi.models.TemplateList;
import io.confluent.idesidecar.scaffolding.TemplateEngine;
import io.confluent.idesidecar.scaffolding.TemplateRegistryService;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.confluent.idesidecar.scaffolding.util.ZipUtil;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Comparator;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path(TemplateResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Templates", description = "Code generation templates")
public class TemplateResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/templates";

  @Inject
  TemplateRegistryService templateRegistryService;

  @Inject
  TemplateEngine templateEngine;

  @GET
  public TemplateList list() {
    var templateManifests = templateRegistryService.listTemplates();
    var templateList = templateManifests.stream()
        .map(Template::new)
        .sorted(Comparator.comparing(Template::getId))
        .toList();
    return new TemplateList(templateList);
  }

  @GET
  @Path("{name}")
  public Template get(String name) throws TemplateRegistryException {
    return new Template(templateRegistryService.getTemplate(name));
  }

  @POST
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("{name}/apply")
  @Operation(
      summary = "Apply a template",
      description = "Applies the specified template and returns the result as a zip file."
  )
  @APIResponse(
      responseCode = "200",
      description = "Template applied successfully",
      content = @Content(
          mediaType = MediaType.APPLICATION_OCTET_STREAM,
          schema = @Schema(type = SchemaType.STRING, format = "binary")
      )
  )
  public Response apply(String name, ApplyTemplateRequest applyTemplateRequest)
      throws TemplateRegistryException {
    var renderedTemplateContents = templateEngine.renderTemplateFromRegistry(
        templateRegistryService,
        name,
        applyTemplateRequest.options()
    );

    // NOTE: We buffer the full bytearray in memory here before returning it
    //       This is acceptable for now since we don't expect the template directories
    //       to be outrageously large, if this becomes a problem with high memory footprint,
    //       we can consider streaming the response instead.
    return Response.ok(ZipUtil.createZipArchive(renderedTemplateContents))
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".zip\"")
        .build();
  }
}
