package io.confluent.idesidecar.restapi.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.idesidecar.restapi.util.RateLimitState;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.logging.Log;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link RestService} that runs its own retry loop so it can honor {@code Retry-After} on 429
 * and 503 responses. The library's loop retries the same statuses after a random wait and never
 * reads the header, and its {@link RestClientException} doesn't carry headers, so this class
 * turns the library's retries off and reads the header from the request's connection instead.
 *
 * <p>Retries keep the library's {@code max.retries} count and full-jitter backoff
 * ({@code retries.wait.ms} doubling up to {@code retries.max.wait.ms}). On 429 or 503 with a
 * {@code Retry-After}, the wait is that value instead. A {@code Retry-After} longer than
 * {@code retries.max.wait.ms}, or a wait that would pass the caller's deadline (see
 * {@link #withDeadline}), fails right away instead of retrying early. Under a deadline, each
 * request's connect and read timeouts are also cut to the time left, on the default
 * {@link HttpURLConnection} path.
 */
public class SidecarRestService extends RestService {

  private static final ThreadLocal<Instant> DEADLINE = new ThreadLocal<>();

  /**
   * The connection of the request in flight on this thread. Only the default
   * {@link HttpURLConnection} path sets it; the optional Apache client path falls back to plain
   * backoff.
   */
  private final ThreadLocal<HttpURLConnection> connection = new ThreadLocal<>();

  private int maxRetries = SchemaRegistryClientConfig.MAX_RETRIES_DEFAULT;
  private Duration initialWait =
      Duration.ofMillis(SchemaRegistryClientConfig.RETRIES_WAIT_MS_DEFAULT);
  private Duration maxWait =
      Duration.ofMillis(SchemaRegistryClientConfig.RETRIES_MAX_WAIT_MS_DEFAULT);

  /**
   * Create a service for the Schema Registry at {@code baseUrl}.
   */
  public SidecarRestService(String baseUrl) {
    super(baseUrl);
  }

  /**
   * A call to a Schema Registry client.
   */
  @FunctionalInterface
  public interface SchemaRegistryCall<T> {

    T call() throws IOException, RestClientException;
  }

  /**
   * Run {@code call} on this thread so that no request it makes, and no wait between its retries,
   * runs past {@code deadline}.
   */
  public static <T> T withDeadline(Instant deadline, SchemaRegistryCall<T> call)
      throws IOException, RestClientException {
    var outer = DEADLINE.get();
    DEADLINE.set(outer != null && outer.isBefore(deadline) ? outer : deadline);
    try {
      return call.call();
    } finally {
      if (outer == null) {
        DEADLINE.remove();
      } else {
        DEADLINE.set(outer);
      }
    }
  }

  @Override
  public void configure(Map<String, ?> configs) {
    maxRetries = SchemaRegistryClientConfig.getMaxRetries(configs);
    initialWait = Duration.ofMillis(SchemaRegistryClientConfig.getRetriesWaitMs(configs));
    maxWait = Duration.ofMillis(SchemaRegistryClientConfig.getRetriesMaxWaitMs(configs));
    var withoutLibraryRetries = new HashMap<String, Object>(configs);
    withoutLibraryRetries.put(SchemaRegistryClientConfig.MAX_RETRIES_CONFIG, 0);
    super.configure(withoutLibraryRetries);
  }

  @Override
  protected HttpURLConnection buildConnection(
      URL url,
      String method,
      Map<String, String> requestProperties
  ) throws IOException {
    var built = super.buildConnection(url, method, requestProperties);
    var deadline = DEADLINE.get();
    if (deadline != null) {
      var leftMs = Duration.between(Instant.now(), deadline).toMillis();
      if (leftMs <= 0) {
        throw new SocketTimeoutException(
            "The deadline for Schema Registry request %s has passed".formatted(url.getPath())
        );
      }
      // each connect and each read waits at most until the deadline (a read timeout is per read)
      built.setConnectTimeout(atMost(built.getConnectTimeout(), leftMs));
      built.setReadTimeout(atMost(built.getReadTimeout(), leftMs));
    }
    connection.set(built);
    return built;
  }

  /**
   * The smaller of an {@link HttpURLConnection} timeout, where 0 means none, and {@code maxMs}.
   */
  private static int atMost(int timeoutMs, long maxMs) {
    return (int) (timeoutMs == 0 ? maxMs : Math.min(timeoutMs, maxMs));
  }

  @Override
  public <T> T httpRequest(
      String path,
      String method,
      byte[] requestBodyData,
      Map<String, String> requestProperties,
      TypeReference<T> responseFormat
  ) throws IOException, RestClientException {
    for (int retries = 0; ; retries++) {
      try {
        return super.httpRequest(path, method, requestBodyData, requestProperties, responseFormat);
      } catch (RestClientException e) {
        if (!isRestClientExceptionRetriable(e)) {
          throw e;
        }
        var retryAfter = retryAfter(e);
        var wait = retryAfter.isPresent() ? retryAfter.get() : backoff(retries);
        var stop = reasonToStop(retries, wait, retryAfter.isPresent());
        if (stop.isPresent()) {
          if (retryAfter.isEmpty()) {
            throw e;
          }
          throw new RetryAfterException(e, retryAfter.get(), stop.get());
        }
        sleep(wait, path, e);
      } catch (IOException e) {
        // like the library's own loop, retry any I/O error
        if (Thread.currentThread().isInterrupted()) {
          throw e;
        }
        var wait = backoff(retries);
        if (reasonToStop(retries, wait, false).isPresent()) {
          throw e;
        }
        sleep(wait, path, e);
      } finally {
        connection.remove();
      }
    }
  }

  /**
   * The {@code Retry-After} of a 429 or 503 response, read from the connection that received it.
   */
  private Optional<Duration> retryAfter(RestClientException e) {
    var received = connection.get();
    if (received == null || (e.getStatus() != 429 && e.getStatus() != 503)) {
      return Optional.empty();
    }
    var header = received.getHeaderField(RateLimitState.HEADER_RETRY_AFTER);
    var seconds = RateLimitState.parseRetryAfter(header, Instant.now());
    // Retry-After: 0 asks for nothing beyond our own backoff
    return seconds <= 0 ? Optional.empty() : Optional.of(Duration.ofSeconds(seconds));
  }

  /**
   * Why the request should fail instead of waiting {@code wait} and retrying, if it should.
   */
  private Optional<String> reasonToStop(int retries, Duration wait, boolean honoringRetryAfter) {
    if (retries >= maxRetries) {
      return Optional.of("retries ran out");
    }
    if (honoringRetryAfter && wait.compareTo(maxWait) > 0) {
      return Optional.of("the longest allowed wait is " + RetryAfterException.describe(maxWait));
    }
    var deadline = DEADLINE.get();
    var left = deadline == null ? null : Duration.between(Instant.now(), deadline);
    if (left != null && wait.compareTo(left) > 0) {
      return Optional.of("only %s were left to wait".formatted(RetryAfterException.describe(left)));
    }
    return Optional.empty();
  }

  private static void sleep(Duration wait, String path, Exception failure)
      throws InterruptedIOException {
    Log.debugf(
        "Retrying Schema Registry request %s in %dms after: %s",
        path, wait.toMillis(), failure.getMessage()
    );
    try {
      Thread.sleep(wait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      var interrupted = new InterruptedIOException(
          "Interrupted while waiting to retry Schema Registry request " + path
      );
      // not the cause: ExceptionUtil.unwrap follows causes and would report the server error
      interrupted.addSuppressed(failure);
      throw interrupted;
    }
  }

  /**
   * Full-jitter exponential backoff, as in the Schema Registry client's own retry loop.
   */
  private Duration backoff(int retries) {
    var ceiling = initialWait.multipliedBy(1L << Math.min(retries, 30));
    if (ceiling.compareTo(maxWait) > 0) {
      ceiling = maxWait;
    }
    var ceilingMs = ceiling.toMillis();
    return ceilingMs <= 0
        ? Duration.ZERO
        : Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceilingMs) + 1);
  }
}
