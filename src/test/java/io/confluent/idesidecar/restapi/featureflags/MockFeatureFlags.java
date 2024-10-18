/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.featureflags;

import io.quarkus.logging.Log;
import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.util.List;

/**
 * An alternative "testable" implementation of {@link FeatureFlags} that can be used in tests
 * and that does not submit calls to LaunchDarkly. This must be defined in {@code src/test/java}
 * and be annotated with {@link Mock @Mock}.
 *
 * <p>This will be used for all {@link jakarta.inject.Inject @Inject}-ed {@link FeatureFlags}
 * fields in JUnit tests.
 *
 * <p>Unfortunately, this does not prevent the instantiation of the regular {@link FeatureFlags}
 * instance, so test logs will have errors like:
 * <pre>
 *   Error evaluating feature flags for project 'Confluent Cloud' ...
 *   Error evaluating feature flags for project 'IDE' ...
 * </pre>
 */
@Mock
@ApplicationScoped
public class MockFeatureFlags extends FeatureFlags {

  public MockFeatureFlags() {
    super(
        List.of(),      // no projects will use device context
        List.of(),      // no projects will use user context
        Duration.ZERO,  // disable the scheduled refreshes
        defaults -> defaults.addAll(FlagEvaluations.defaults())
    );
    Log.info("Instantiating mock feature flags");
  }

  @Override
  public String toString() {
    return "MockFeatureFlags";
  }
}
