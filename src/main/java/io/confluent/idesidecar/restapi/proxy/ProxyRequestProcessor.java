package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;

/**
 * Generic processor that ships the request to the target server and updates the context with the
 * response.
 *
 * @param <T> The type of the context that must extend {@link ProxyContext}
 */
public class ProxyRequestProcessor<T extends ProxyContext> extends
    Processor<T, Future<T>> {

  ProxyHttpClient<T> proxyHttpClient;

  public ProxyRequestProcessor(WebClientFactory webClientFactory) {
    proxyHttpClient = new ProxyHttpClient<>(webClientFactory);
  }

  @Override
  public Future<T> process(T context) {
    return proxyHttpClient.send(context).compose(
        processedContext -> next().process(processedContext)
    );
  }
}
