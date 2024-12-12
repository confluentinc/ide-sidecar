package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

/**
 * HTTP client used when proxying requests to the Kafka REST and Schema Registry APIs.
 */
public class ProxyHttpClient<T extends ProxyContext> {
  WebClientFactory webClientFactory;
  Vertx vertx;

  public ProxyHttpClient(WebClientFactory webClientFactory, Vertx vertx) {
    this.webClientFactory = webClientFactory;
    this.vertx = vertx;
  }

  public Future<T> send(T context) {
    var options = webClientFactory.getDefaultWebClientOptions();
    if (context.getTruststoreOptions() != null) {
      options.setTrustStoreOptions(context.getTruststoreOptions());
    }

    return WebClient.create(vertx, options)
        .requestAbs(context.getProxyRequestMethod(), context.getProxyRequestAbsoluteUrl())
        .putHeaders(context.getProxyRequestHeaders())
        .sendBuffer(context.getProxyRequestBody())
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
