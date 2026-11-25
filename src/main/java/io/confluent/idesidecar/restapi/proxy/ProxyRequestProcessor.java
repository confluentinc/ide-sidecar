package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/**
 * Processor that ships the request to the target server and updates the context with the response.
 */
@ApplicationScoped
public class ProxyRequestProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private final ProxyHttpClient<ProxyContext> proxyHttpClient;
  private final Duration timeout;

  public ProxyRequestProcessor(WebClientFactory webClientFactory, Vertx vertx) {
    this(webClientFactory, vertx, WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_SECONDS);
  }

  /**
   * A processor whose requests fail with 504 if they don't finish within {@code timeout}.
   */
  public ProxyRequestProcessor(WebClientFactory webClientFactory, Vertx vertx, Duration timeout) {
    proxyHttpClient = new ProxyHttpClient<>(webClientFactory, vertx);
    this.timeout = timeout;
  }

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    return proxyHttpClient.send(context, timeout).compose(
        processedContext -> {
          return next().process(processedContext);
        });
  }
}
