package io.confluent.idesidecar.restapi.util;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.impl.HttpContext;
import io.vertx.ext.web.client.impl.WebClientInternal;
import java.util.concurrent.TimeoutException;

/**
 * Makes a request's {@code HttpRequest.timeout(t)} cap the whole exchange: getting a connection,
 * the TLS handshake, and the full response, however slowly it arrives. On its own, Vert.x applies
 * {@code t} to getting a connection and then again to each silence in the response, so a call can
 * run well past {@code t}. When the deadline passes, the request fails with a
 * {@link TimeoutException} and is reset, which closes its connection.
 */
final class RequestDeadlineInterceptor implements Handler<HttpContext<?>> {

  private static final String DEADLINE = RequestDeadlineInterceptor.class.getName();

  private final Vertx vertx;

  private RequestDeadlineInterceptor(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Add the deadline to every request {@code client} sends.
   */
  static WebClient install(WebClient client, Vertx vertx) {
    ((WebClientInternal) client).addInterceptor(new RequestDeadlineInterceptor(vertx));
    return client;
  }

  @Override
  public void handle(HttpContext<?> context) {
    Deadline deadline = context.get(DEADLINE);
    switch (context.phase()) {
      case CREATE_REQUEST -> {
        var timeoutMs = context.requestOptions().getTimeout();
        if (deadline == null && timeoutMs > 0) {
          context.set(DEADLINE, new Deadline(context, timeoutMs));
        } else if (deadline != null) {
          // a redirect, which stays under the first request's deadline
          deadline.forgetRequest();
        }
      }
      case SEND_REQUEST -> {
        if (deadline != null && !deadline.track(context.clientRequest())) {
          // the deadline passed while this request waited for a connection, and the caller
          // already has its failure
          return;
        }
      }
      case RECEIVE_RESPONSE -> {
        if (deadline != null) {
          // the body can end a tick before the response is dispatched
          context.clientResponse().end().onComplete(ar -> deadline.finish());
        }
      }
      case DISPATCH_RESPONSE, FAILURE -> {
        if (deadline != null) {
          deadline.finish();
        }
      }
      default -> {
        // nothing to do in the other phases
      }
    }
    context.next();
  }

  private final class Deadline {

    private final HttpContext<?> context;
    private final TimeoutException timeout;
    private final long timerId;
    private HttpClientRequest request;
    private boolean passed;
    private boolean finished;

    Deadline(HttpContext<?> context, long timeoutMs) {
      this.context = context;
      var options = context.requestOptions();
      this.timeout = new TimeoutException("The timeout of %dms ran out before %s %s finished"
          .formatted(timeoutMs, options.getMethod(), options.getURI()));
      // runs on this request's context, so it never overlaps the phases above
      this.timerId = vertx.setTimer(timeoutMs, id -> pass());
    }

    /**
     * Track the request now being sent, or reset it and return false if the deadline already
     * passed.
     */
    boolean track(HttpClientRequest request) {
      if (passed) {
        request.reset(0, timeout);
        return false;
      }
      this.request = request;
      return true;
    }

    /**
     * Stop tracking a finished request, so the deadline can't reset it.
     */
    void forgetRequest() {
      request = null;
    }

    /**
     * Stop the deadline once the exchange is over. Resetting a request after its response ended
     * would close a keep-alive connection that another request may already be using.
     */
    void finish() {
      finished = true;
      request = null;
      vertx.cancelTimer(timerId);
    }

    private void pass() {
      if (finished) {
        return;
      }
      passed = true;
      // the failure runs the FAILURE phase right away, which finishes this deadline
      var inFlight = request;
      // fail first, so the caller sees the timeout and not the reset it causes
      context.fail(timeout);
      if (inFlight != null) {
        inFlight.reset(0, timeout);
      }
    }
  }
}
