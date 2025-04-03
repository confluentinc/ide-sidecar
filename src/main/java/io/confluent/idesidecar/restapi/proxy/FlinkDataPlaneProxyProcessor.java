package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.SanitizeHeadersUtil.sanitizeHeaders;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.UriUtil;
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
  public UriUtil uriUtil;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    MultiMap headers = context.getRequestHeaders() != null ?
        context.getRequestHeaders() : MultiMap.caseInsensitiveMultiMap();

    String region = headers.get(REGION_HEADER);
    String provider = headers.get(PROVIDER_HEADER);

    if (region != null && provider != null) {
      // Build the Flink URL correctly
      String flinkBaseUrl = flinkUrlPattern.formatted(region.toLowerCase(), provider.toLowerCase());
      String path = context.getRequestUri();

      // Ensure we have a proper URL
      String absoluteUrl = uriUtil.combine(flinkBaseUrl, path);
      context.setProxyRequestAbsoluteUrl(absoluteUrl);

      var cleanedHeaders = sanitizeHeaders(headers, httpHeaderExclusions);

      // Create a new MultiMap from the cleaned headers
      MultiMap proxyHeaders = MultiMap.caseInsensitiveMultiMap();
      cleanedHeaders.forEach(proxyHeaders::add);

      //Set the properly typed headers
      context.setProxyRequestHeaders(proxyHeaders);
      context.setProxyRequestMethod(context.getRequestMethod());
      context.setProxyRequestBody(context.getRequestBody());

      return next().process(context);
    } else {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(400, "Missing required headers: x-ccloud-region and x-ccloud-provider are required for Flink requests")
          )
      );
    }
  }
}
