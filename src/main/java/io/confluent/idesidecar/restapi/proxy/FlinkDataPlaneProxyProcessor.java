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
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Processor to set the proxy request/response parameters for the data plane.
 */
@ApplicationScoped
public class FlinkDataPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final String REGION_HEADER = "x-ccloud-region";
  private static final String PROVIDER_HEADER = "x-ccloud-provider";

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

    // Get environment ID
    String environmentId = extractEnvIdFromPath(context.getRequestUri());

    // Get private endpoints for this environment
    List<String> privateEndpoints = flinkPrivateEndpointUtil.getPrivateEndpoints(environmentId);

    String flinkBaseUrl;
    if (!privateEndpoints.isEmpty()) {
      String selectedEndpoint = selectMatchingEndpoint(privateEndpoints, region, providerStr);
      if (selectedEndpoint != null) {
        flinkBaseUrl = selectedEndpoint;
      } else {
        // No matching private endpoint, fall back to public pattern
        flinkBaseUrl = flinkUrlPattern.formatted(region.toLowerCase(), providerStr.toLowerCase());
      }
    } else {
      // No private endpoints configured, use public pattern
      flinkBaseUrl = flinkUrlPattern.formatted(region.toLowerCase(), providerStr.toLowerCase());
    }

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

  private String extractEnvIdFromPath(String uri) {
    if (uri == null) return null;
    if (uri.contains("/environments/")) {
      return uri.split("/environments/")[1].split("/")[0].split("\\?")[0];
    }
    if (uri.contains("environment=")) {
      return uri.split("environment=")[1].split("&")[0];
    }
    return null;
  }

  /**
   * Selects the first endpoint that matches the given region and provider.
   */
  private String selectMatchingEndpoint(List<String> endpoints, String region, String provider) {
    return endpoints.stream()
        .filter(endpoint -> matchesRegionAndProvider(endpoint, region, provider))
        .findFirst()
        .orElse(null);
  }

  /**
   * Checks if an endpoint matches the given region and provider.
   * Supports both formats:
   * - flink.{region}.{provider}.private.confluent.cloud
   * - flink.{domain}.{region}.{provider}.private.confluent.cloud
   */
  private boolean matchesRegionAndProvider(String endpoint, String region, String provider) {
    if (endpoint == null || region == null || provider == null) {
      return false;
    }

    // Remove protocol prefix and split by dots
    String cleanEndpoint = endpoint.replaceFirst("^https?://", "");
    String[] parts = cleanEndpoint.split("\\.");

    // Must end with .private.confluent.cloud (3 parts)
    if (parts.length < 6) return false;

    // Extract region and provider - always 5th and 4th from the end
    // "https://flink.dom123.us-east-1.aws.private.confluent.cloud"
    // "http://flink.us-west-2.aws.private.confluent.cloud"
    String endpointRegion = parts[parts.length - 5];   // e.g. us-east-1
    String endpointProvider = parts[parts.length - 4]; // e.g. aws

    return region.equalsIgnoreCase(endpointRegion) && provider.equalsIgnoreCase(endpointProvider);
  }
}
