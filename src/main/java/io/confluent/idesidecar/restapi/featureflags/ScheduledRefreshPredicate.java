/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.featureflags;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.ScheduledExecution;
import java.time.Duration;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * The {@link io.quarkus.scheduler.Scheduled.SkipPredicate} implementation used to determine whether
 * the {@link FeatureFlags#refreshFlags()} method should be scheduled.
 * This must be public in order for the {@link Scheduled} annotation to access it.
 */
@RegisterForReflection
public class ScheduledRefreshPredicate implements Scheduled.SkipPredicate {

  /**
   * Configuration property that controls the interval between scheduled refreshes, via the
   * {@code ide-sidecar.feature-flags.refresh-interval-seconds} application property.
   * Refreshes will only be scheduled when the value is positive,
   * and the default is 0 (no automatic refreshes).
   */
  static final Duration SCHEDULED_REFRESH_INTERVAL = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getOptionalValue("ide-sidecar.feature-flags.refresh-interval-seconds", Long.class)
          .orElse(0L)
  );

  @Override
  public boolean test(ScheduledExecution execution) {
    return SCHEDULED_REFRESH_INTERVAL.isPositive();
  }
}
