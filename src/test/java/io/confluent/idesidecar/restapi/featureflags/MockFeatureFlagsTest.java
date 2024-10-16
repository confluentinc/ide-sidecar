package io.confluent.idesidecar.restapi.featureflags;

import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_SENTRY_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class MockFeatureFlagsTest {

  @Inject
  FeatureFlags flags;

  @Test
  public void shouldUseOverriddenFlag() {
    // Check the default value (this is not necessary in real tests)
    assertTrue(flags.evaluateAsBoolean(IDE_SENTRY_ENABLED));

    // When the flag is overridden
    flags.overrides().add(IDE_SENTRY_ENABLED, false);

    // Then evaluations will be false
    assertFalse(flags.evaluateAsBoolean(IDE_SENTRY_ENABLED));
  }

  @Test
  void shouldHaveMockFeatureFlags() {
    assertEquals("MockFeatureFlags", flags.toString());
  }
}
