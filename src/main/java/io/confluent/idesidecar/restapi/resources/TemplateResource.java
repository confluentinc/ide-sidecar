package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.cloud.scaffold.v1.model.Failure;
import io.confluent.cloud.scaffold.v1.model.ScaffoldV1Template;
import io.confluent.cloud.scaffold.v1.model.ScaffoldV1TemplateList;
import io.confluent.idesidecar.restapi.exceptions.ScaffoldingException;
import io.confluent.idesidecar.restapi.models.ApplyTemplateRequest;
import io.confluent.idesidecar.restapi.models.Template;
import io.confluent.idesidecar.restapi.models.TemplateList;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * API endpoints related to the scaffolding functionality.
 */
@Path(TemplateResource.API_RESOURCE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "Templates", description = "Code generation templates")
@Blocking
@ApplicationScoped
public class TemplateResource {

  public static final String API_RESOURCE_PATH = "/gateway/v1/templates";

  static final String SCAFFOLDING_SERVICE_LIST_TEMPLATES_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.scaffolding-service.list-templates-uri", String.class);
  static final String SCAFFOLDING_SERVICE_GET_TEMPLATE_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.scaffolding-service.get-template-uri", String.class);
  static final String SCAFFOLDING_SERVICE_APPLY_TEMPLATE_URI = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.scaffolding-service.apply-template-uri", String.class);

  static final MultiMap NO_HEADERS = MultiMap.caseInsensitiveMultiMap();
  static final Map<String, String> NO_PATH_PARAMS = Map.of();

  static final ProxyContext LIST_TEMPLATES_PROXY_CONTEXT = new ProxyContext(
      SCAFFOLDING_SERVICE_LIST_TEMPLATES_URI,
      NO_HEADERS,
      HttpMethod.GET,
      null,
      NO_PATH_PARAMS,
      // No connection ID
      null
  );

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Inject
  @Named("scaffoldingProxyProcessor")
  Processor<ProxyContext, Future<ProxyContext>> scaffoldingProxyProcessor;

  @GET
  public Uni<TemplateList> list() {
    return uniStage(() -> scaffoldingProxyProcessor.process(LIST_TEMPLATES_PROXY_CONTEXT).toCompletionStage())
        .chain(context -> parseJson(
            context.getProxyResponseBody().toString(), ScaffoldV1TemplateList.class))
        .map(TemplateList::new);
  }

  @GET
  @Path("{name}")
  public Uni<Template> get(String name) {
    var proxyContext = new ProxyContext(
        SCAFFOLDING_SERVICE_GET_TEMPLATE_URI.formatted(name),
        NO_HEADERS,
        HttpMethod.GET,
        null,
        NO_PATH_PARAMS,
        null
    );
    return uniStage(() -> scaffoldingProxyProcessor.process(proxyContext).toCompletionStage())
        .chain(context -> parseJson(
                context.getProxyResponseBody().toString(), ScaffoldV1Template.class)
        )
        .map(Template::new);
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
  public Uni<Response> apply(String name, ApplyTemplateRequest applyTemplateRequest) {
    byte[] buffer;
    try {
      buffer = OBJECT_MAPPER.writeValueAsBytes(
          applyTemplateRequest.toApplyScaffoldV1TemplateRequest());
    } catch (JsonProcessingException e) {
      throw new ScaffoldingException(e);
    }

    var proxyContext = new ProxyContext(
        SCAFFOLDING_SERVICE_APPLY_TEMPLATE_URI.formatted(name),
        NO_HEADERS,
        HttpMethod.POST,
        Buffer.buffer(buffer),
        NO_PATH_PARAMS,
        null
    );

    return uniStage(() -> scaffoldingProxyProcessor.process(proxyContext).toCompletionStage())
        .chain(context -> {
          var resp = Response
              .status(context.getProxyResponseStatusCode())
              .entity(context.getProxyResponseBody().getBytes());
          if (context.getProxyResponseHeaders() != null) {
            context.getProxyResponseHeaders().forEach(
                header -> resp.header(header.getKey(), header.getValue())
            );
          }
          // Set the Content-Disposition header to suggest the browser to download the file
          resp.header(
              HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".zip\""
          );
          return uniItem(resp.build());
        });
  }

  private <T> Uni<T> parseJson(String json, Class<T> clazz) {
    try {
      return uniItem(OBJECT_MAPPER.readValue(json, clazz));
    } catch (JsonProcessingException e) {
      try {
        // Try to parse the response as a Scaffold V1 failure, wrapped in a ProcessorFailedException
        return Uni.createFrom().failure(
            mapScaffoldV1Failure(OBJECT_MAPPER.readValue(json, Failure.class))
        );
      } catch (JsonProcessingException e2) {
        // We can't parse the response as a failure either, throw an exception
        throw new ScaffoldingException(e2);
      }
    }
  }

  private static ScaffoldingException mapScaffoldV1Failure(Failure failure) {
    return new ScaffoldingException(
        new io.confluent.idesidecar.restapi.exceptions.Failure(
            failure.getErrors().stream()
                .map(error -> new io.confluent.idesidecar.restapi.exceptions.Failure.Error(
                    error.getCode(),
                    error.getStatus(),
                    error.getTitle(),
                    error.getDetail(),
                    getSource(error)
                ))
                .toList()
        )
    );
  }

  private static Map<String, ?> getSource(io.confluent.cloud.scaffold.v1.model.Error error) {
    if (error.getSource() == null) {
      return null;
    } else {
      var source = new HashMap<String, String>();
      if (error.getSource().getPointer() != null) {
        source.put("pointer", error.getSource().getPointer());
      }
      if (error.getSource().getParameter() != null) {
        source.put("parameter", error.getSource().getParameter());
      }
      return source;
    }
  }
}
