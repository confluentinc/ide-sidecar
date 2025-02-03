package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Processor to set the proxy request/response parameters for RBAC.
 */
@ApplicationScoped
public class RBACProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final Logger logger = Logger.getLogger(RBACProxyProcessor.class.getName());

  @Override
  public Future<ProxyContext> process(ProxyContext context) {

    // Log the body
    logger.log(Level.INFO, "Request Body FROM RBAC: {0}", context.getRequestBody());

    context.setProxyRequestAbsoluteUrl(context.getRequestUri());
    context.setProxyRequestHeaders(context.getProxyRequestHeaders());
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context);
  }
}