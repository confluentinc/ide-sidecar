package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;

/**
 * HTTP client used when proxying requests to the Kafka REST and Schema Registry APIs.
 */
public class ProxyHttpClient<T extends ProxyContext> {
  WebClientFactory webClientFactory;

  public ProxyHttpClient(WebClientFactory webClientFactory) {
    this.webClientFactory = webClientFactory;
  }

  public Future<T> send(T context) {
    return webClientFactory.getWebClient()
        .requestAbs(context.proxyRequestMethod, context.proxyRequestAbsoluteUrl)
        .putHeaders(context.proxyRequestHeaders)
        .sendBuffer(context.proxyRequestBody)
        .compose(
            // If success, update context and pass it to the next processor
            // Success means we were able to make a call to the server & we got a response.
            proxyResponse -> {
              context.setProxyResponseHeaders(proxyResponse.headers());
              context.setProxyResponseStatusCode(proxyResponse.statusCode());
              context.setProxyResponseBody(proxyResponse.body());
              return Future.succeededFuture(context);
            },
            // If failure, stop the chain and return a failed future.
            // This covers cases where the request was not sent to the server, or we failed to
            // get a response.
            throwable -> {
              var failure = context
                  .error("proxy_error", throwable.getMessage())
                  .failf(500, "Something went wrong while proxying request");
              return Future.failedFuture(new ProcessorFailedException(failure));
            }
        );
  }
}
