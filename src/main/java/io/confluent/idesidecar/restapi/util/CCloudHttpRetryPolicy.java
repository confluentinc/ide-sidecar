package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.exceptions.TooManyRequestsException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Centralizes the exponential-backoff + jitter retry policy applied to outbound CCloud calls
 * that throw {@link TooManyRequestsException}. Used by both the GraphQL fetcher path
 * ({@code ConfluentCloudRestClient.wrapRequest}) and the auth path
 * ({@code CCloudOAuthContext.sendWithRateLimit}) so both follow the same retry contract.
 *
 * <p>The acquire/release lifecycle around the limiter is intentionally NOT in this class -
 * callers wire that explicitly so the read order of cause-and-effect at each call site is
 * obvious.
 */
@ApplicationScoped
@RegisterForReflection
public class CCloudHttpRetryPolicy {

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.initial-backoff-ms",
      defaultValue = "500"
  )
  long initialBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-backoff-ms",
      defaultValue = "10000"
  )
  long maxBackoffMs;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.max-retries",
      defaultValue = "5"
  )
  int maxRetries;

  @ConfigProperty(
      name = "ide-sidecar.connections.ccloud.rate-limit.retry.jitter-factor",
      defaultValue = "0.2"
  )
  double jitterFactor;

  /**
   * Wrap {@code source} with retry-on-{@link TooManyRequestsException} using the configured
   * exponential backoff + jitter policy. Logs at ERROR on retry exhaustion so operators can
   * distinguish exhausted retries from a single unretried 429.
   */
  public <T> Uni<T> applyRetry(String url, Uni<T> source) {
    return source
        .onFailure(TooManyRequestsException.class)
        .retry()
        .withBackOff(
            Duration.ofMillis(initialBackoffMs),
            Duration.ofMillis(maxBackoffMs)
        )
        .withJitter(jitterFactor)
        .atMost(maxRetries)
        .onFailure(TooManyRequestsException.class)
        .invoke(t -> Log.errorf(
            "Rate-limit retries exhausted for %s after %d retries; propagating failure",
            url, maxRetries
        ));
  }
}
