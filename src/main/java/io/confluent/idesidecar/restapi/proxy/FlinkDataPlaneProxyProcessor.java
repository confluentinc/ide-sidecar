package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.SanitizeHeadersUtil.sanitizeHeaders;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Processor to set the proxy request/response parameters for the data plane.
 */
@ApplicationScoped
public class FlinkDataPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final String REGION_HEADER = "x-ccloud-region";
  private static final String PROVIDER_HEADER = "x-ccloud-provider";
  private static final String FLINK_URL_PATTERN = "https://flink.%s.%s.confluent.cloud";

  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    MultiMap headers = context.getRequestHeaders() != null ?
        context.getRequestHeaders() : MultiMap.caseInsensitiveMultiMap();

    String region = headers.get(REGION_HEADER);
    String provider = headers.get(PROVIDER_HEADER);
    UriUtil uriUtil = new UriUtil();

    if (region != null && provider != null) {
      // Build the Flink URL correctly
      String flinkBaseUrl = FLINK_URL_PATTERN.formatted(region.toLowerCase(), provider.toLowerCase());
      String path = context.getRequestUri();

      // Make sure path starts with a / if not already, else it will fail with a 404
      if (!path.startsWith("/")) {
        path = "/" + path;
      }

      // Ensure we have a proper URL
      String absoluteUrl = uriUtil.combine(flinkBaseUrl, path);
      context.setProxyRequestAbsoluteUrl(absoluteUrl);

      // Create a copy of headers to modify
      MultiMap cleanedHeaders = MultiMap.caseInsensitiveMultiMap();
      cleanedHeaders.addAll(headers);

      // Explicitly remove the Flink-specific headers
      sanitizeHeaders(cleanedHeaders,httpHeaderExclusions);
      cleanedHeaders.remove("x-connection-id");
      cleanedHeaders.remove("host");

      // Apply other exclusions
      for (String exclusion : httpHeaderExclusions) {
        cleanedHeaders.remove(exclusion);
      }

      cleanedHeaders.set("Accept", "application/json");
      if (context.getRequestMethod().name().equals("POST") ||
          context.getRequestMethod().name().equals("PUT")) {
        cleanedHeaders.set("Content-Type", "application/json");
      }

      context.setProxyRequestHeaders(cleanedHeaders);
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
