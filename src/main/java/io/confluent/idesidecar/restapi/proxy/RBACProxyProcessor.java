package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Processor to set the proxy request/response parameters for RBAC.
 */
@ApplicationScoped
public class RBACProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  @Override
  public Future<ProxyContext> process(ProxyContext context) {

    context.setProxyRequestAbsoluteUrl(context.getRequestUri());
    context.setProxyRequestHeaders(context.getRequestHeaders());
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context);
  }
}