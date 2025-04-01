package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Named;
import org.jboss.logging.Logger;

/**
 * Processor to set the proxy request/response parameters for the control plane.
 */
@ApplicationScoped
public class ControlPlaneProxyProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private static final Logger LOG = Logger.getLogger(ControlPlaneProxyProcessor.class);

  private Processor<ProxyContext, Future<ProxyContext>> next;

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    var headers = context.getProxyRequestHeaders() != null ? context.getProxyRequestHeaders()
        : MultiMap.caseInsensitiveMultiMap();
    context.setProxyRequestHeaders(headers);
    context.setProxyRequestAbsoluteUrl(context.getRequestUri());
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());
    return next.process(context);
  }

  public void setNext(Processor<ProxyContext, Future<ProxyContext>> nextProcessor) {
    this.next = nextProcessor;
  }
}