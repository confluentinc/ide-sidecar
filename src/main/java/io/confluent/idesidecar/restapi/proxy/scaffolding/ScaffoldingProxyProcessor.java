package io.confluent.idesidecar.restapi.proxy.scaffolding;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Processor to set the proxy request/response parameters for the CCloud Scaffolding Service API.
 */
@ApplicationScoped
public class ScaffoldingProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    // Set up the proxy request parameters for the Scaffolding Service API
    context.setProxyRequestAbsoluteUrl(context.getRequestUri());
    context.setProxyRequestHeaders(context.getRequestHeaders());
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context);
  }
}
