package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.SanitizeHeadersUtil.sanitizeHeaders;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.graph.CloudProvider;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.confluent.idesidecar.restapi.util.FlinkPrivateEndpointUtil;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Processor to set the proxy request/response parameters for the data plane.
 */
@ApplicationScoped
public class FlinkDataPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final String REGION_HEADER = "x-ccloud-region";
  private static final String PROVIDER_HEADER = "x-ccloud-provider";
  private static final String ENVIRONMENT_HEADER = "x-ccloud-env-id";

  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  @ConfigProperty(name = "ide-sidecar.flink.url-pattern")
  String flinkUrlPattern;

  @Inject
  UriUtil uriUtil;

  @Inject
  FlinkPrivateEndpointUtil flinkPrivateEndpointUtil;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    MultiMap headers = context.getRequestHeaders() != null ?
        context.getRequestHeaders() : MultiMap.caseInsensitiveMultiMap();

    String region = headers.get(REGION_HEADER);
    String providerStr = headers.get(PROVIDER_HEADER);
    CloudProvider provider = CloudProvider.of(providerStr);
    String environmentId = headers.get(ENVIRONMENT_HEADER);

    if (CloudProvider.NONE.equals(provider)) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "CCloud provider specified in request header '%s' is missing or invalid.".formatted(PROVIDER_HEADER))
          )
      );
    }

    if (region == null || region.isBlank()) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Region specified in request header '%s' is missing or invalid.".formatted(REGION_HEADER))
          )
      );
    }

    // Get private endpoints for current environment
    List<String> privateEndpoints = flinkPrivateEndpointUtil.getPrivateEndpoints(environmentId);

    // Try private endpoint first, then fallback to public endpoint
    String flinkBaseUrl = selectMatchingEndpoint(privateEndpoints, region, providerStr)
        .orElse(flinkUrlPattern.formatted(region.toLowerCase(), providerStr.toLowerCase()));

    String path = context.getRequestUri();

    String absoluteUrl = uriUtil.combine(flinkBaseUrl, path);
    context.setProxyRequestAbsoluteUrl(absoluteUrl);

    var cleanedHeaders = sanitizeHeaders(headers, httpHeaderExclusions);

    MultiMap proxyHeaders = MultiMap.caseInsensitiveMultiMap();
    cleanedHeaders.forEach(proxyHeaders::add);

    context.setProxyRequestHeaders(proxyHeaders);
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context);
  }

  /**
   * Selects the first endpoint that matches the given region and provider.
   */
  public Optional<String> selectMatchingEndpoint(List<String> endpoints, String region, String provider) {
    return endpoints.stream()
        .filter(endpoint ->
            flinkPrivateEndpointUtil.isValidEndpointWithMatchingRegionAndProvider(
                endpoint, region, provider))
        .findFirst();
  }
}
