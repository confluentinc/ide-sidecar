package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

/**
 * Processor to set the proxy request/response parameters for the data plane.
 */
@ApplicationScoped
public class DataPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final Logger LOG = Logger.getLogger(DataPlaneProxyProcessor.class);
  private static final String REGION_HEADER = "x-ccloud-region";
  private static final String PROVIDER_HEADER = "x-ccloud-provider";
  private static final String FLINK_URL_PATTERN = "https://flink.%s.%s.confluent.cloud";

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    MultiMap headers = context.getRequestHeaders() != null ?
        context.getRequestHeaders() : MultiMap.caseInsensitiveMultiMap();

    if (isFlinkRequest(context)) {
      String region = headers.get(REGION_HEADER);
      String provider = headers.get(PROVIDER_HEADER);

      if (region != null && provider != null) {
        String flinkBaseUrl = String.format(FLINK_URL_PATTERN,
            region.toLowerCase(), provider.toLowerCase());
        String path = extractPathFromUri(context.getRequestUri());

        context.setProxyRequestAbsoluteUrl(flinkBaseUrl + path);

        MultiMap cleanedHeaders = MultiMap.caseInsensitiveMultiMap();
        cleanedHeaders.addAll(headers);

        // Remove the headers that shouldn't be forwarded
        cleanedHeaders.remove(REGION_HEADER);
        cleanedHeaders.remove(PROVIDER_HEADER);
        cleanedHeaders.remove("x-connection-id");
        cleanedHeaders.remove("host");

        cleanedHeaders.set("Accept", "application/json");

        if (context.getRequestMethod().name().equals("POST") ||
            context.getRequestMethod().name().equals("PUT")) {
          cleanedHeaders.set("Content-Type", "application/json");
        }

        context.setProxyRequestHeaders(cleanedHeaders);
      } else {
        context.setProxyRequestAbsoluteUrl(context.getRequestUri());
        context.setProxyRequestHeaders(headers);
      }
    } else {
      context.setProxyRequestAbsoluteUrl(context.getRequestUri());
      context.setProxyRequestHeaders(headers);
    }
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context);
  }
  private boolean isFlinkRequest(ProxyContext context) {
    return context.getRequestUri().contains("sql/v1") ||
        context.getRequestUri().contains("catalog/v1");
  }

  private String extractPathFromUri(String uri) {
    if (uri.contains("sql/v1")) {
      int sqlIndex = uri.indexOf("sql/v1");
      return "/" + uri.substring(sqlIndex);
    }

    if (uri.contains("catalog/v1")) {
      int catalogIndex = uri.indexOf("catalog/v1");
      return "/" + uri.substring(catalogIndex);
    }

    // Fallback to original extraction
    int pathStart = uri.indexOf('/', uri.indexOf("://") + 3);
    return pathStart >= 0 ? uri.substring(pathStart) : "";
  }
}
