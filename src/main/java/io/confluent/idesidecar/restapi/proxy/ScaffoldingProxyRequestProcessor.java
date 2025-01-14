package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Processor that ships the request to the target server and updates the context with the
 * response.
 */
@ApplicationScoped
public class ScaffoldingProxyRequestProcessor extends
    Processor<ProxyContext, Future<ProxyContext>> {

  final ProxyHttpClient<ProxyContext> proxyHttpClient;

  @Inject
  public ScaffoldingProxyRequestProcessor(WebClientFactory webClientFactory, Vertx vertx) {
    proxyHttpClient = new ProxyHttpClient<>(webClientFactory, vertx);
  }

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    return proxyHttpClient.send(context).compose(
        processedContext -> next().process(processedContext)
    );
  }
}
