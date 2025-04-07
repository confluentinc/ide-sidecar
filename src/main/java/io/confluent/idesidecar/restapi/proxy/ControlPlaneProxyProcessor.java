package io.confluent.idesidecar.restapi.proxy;

import static io.confluent.idesidecar.restapi.util.SanitizeHeadersUtil.sanitizeHeaders;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Processor to set the proxy request/response parameters for the control plane.
 */
@ApplicationScoped
public class ControlPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {
  @ConfigProperty(name = "ide-sidecar.cluster-proxy.http-header-exclusions")
  List<String> httpHeaderExclusions;

  private Processor<ProxyContext, Future<ProxyContext>> next;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    var headers = context.getRequestHeaders() != null ? context.getRequestHeaders()
        : MultiMap.caseInsensitiveMultiMap();
    var cleanedHeaders = sanitizeHeaders(headers, httpHeaderExclusions);
    // Create a new MultiMap from the cleaned headers
    var proxyHeaders = MultiMap.caseInsensitiveMultiMap();
    cleanedHeaders.forEach(proxyHeaders::add);

    context.setProxyRequestHeaders(proxyHeaders);
    context.setProxyRequestAbsoluteUrl(context.getRequestUri());
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());
    return next.process(context);
  }

  public void setNext(Processor<ProxyContext, Future<ProxyContext>> nextProcessor) {
    this.next = nextProcessor;
  }
}