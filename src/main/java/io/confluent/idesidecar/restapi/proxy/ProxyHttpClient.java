package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.netty.channel.ConnectTimeoutException;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

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

  /**
   * Send the proxied request with the default request timeout.
   */
  public Future<T> send(T context) {
    return send(context, WebClientFactory.WEBCLIENT_REQUEST_TIMEOUT_SECONDS);
  }

  /**
   * Send the proxied request, failing with 504 if the server sends nothing for {@code timeout}.
   */
  public Future<T> send(T context, Duration timeout) {
    var webClient = context.getWebClientOptions() != null
        ? WebClient.create(vertx, context.getWebClientOptions())
        : webClientFactory.getWebClient();
    return webClient
        .requestAbs(context.getProxyRequestMethod(), context.getProxyRequestAbsoluteUrl())
        .timeout(timeout.toMillis())
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
            // This covers cases where the request was not sent to the server, we failed to
            // get a response, or the server sent nothing for the whole timeout.
            throwable -> {
              Log.errorf(
                  throwable,
                  "Proxied %s %s failed",
                  context.getProxyRequestMethod(),
                  context.getProxyRequestAbsoluteUrl()
              );
              return Future.failedFuture(
                  new ProcessorFailedException(toFailure(context, throwable, timeout))
              );
            }
        );
  }

  /**
   * Map a failed send to the response the caller sees: 504 when the server stalled or couldn't be
   * reached in time, and 500 otherwise.
   */
  static Failure toFailure(ProxyContext context, Throwable throwable, Duration timeout) {
    if (throwable instanceof TimeoutException) {
      return context
          .error("proxy_timeout", throwable.getMessage())
          .failf(
              504,
              "%s didn't respond within %s",
              hostOf(context.getProxyRequestAbsoluteUrl()),
              TimeUtil.humanReadableDuration(timeout)
          );
    } else if (throwable instanceof ConnectTimeoutException) {
      return context
          .error("proxy_timeout", throwable.getMessage())
          .failf(
              504,
              "Couldn't connect to %s in time",
              hostOf(context.getProxyRequestAbsoluteUrl())
          );
    }
    return context
        .error("proxy_error", throwable.getMessage())
        .failf(500, "Something went wrong while proxying request");
  }

  /**
   * The URL's host, or the whole URL when {@link URI} can't parse it (Vert.x's parser is more
   * lenient).
   */
  private static String hostOf(String url) {
    try {
      var host = URI.create(url).getHost();
      return host != null ? host : url;
    } catch (IllegalArgumentException e) {
      Log.errorf(e, "Couldn't read the host of proxied URL %s", url);
      return url;
    }
  }
}
