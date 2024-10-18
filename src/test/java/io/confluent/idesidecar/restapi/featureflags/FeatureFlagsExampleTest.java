package io.confluent.idesidecar.restapi.featureflags;

import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_SENTRY_ENABLED;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * This is an example test case that shows how to use {@link FeatureFlags} in unit or integration
 * tests. This initializes the {@link FeatureFlags} to <i>not</i> use LaunchDarkly, and the
 * <i>default</i> flags are loaded, though your tests can override the flags at any time.
 *
 * <p>This relies upon the {@link MockFeatureFlags} class being defined in {@code /src/test/java}
 * and having the {@link io.quarkus.test.Mock @Mock} annotation that defines it as an
 * <a href="https://quarkus.io/guides/getting-started-testing#cdi-alternative-mechanism">alternative bean</a>
 * that will be used in all tests.
 */
@QuarkusTest
class FeatureFlagsExampleTest {

  @Inject
  FeatureFlags flags;

  @Inject
  ExampleBean exampleBean;

  @AfterEach
  public void afterEach() {
    // If you perform any overrides, be sure to undo them!
    // Failing to do this may cause failures in other test classes.
    flags.overrides().clear();
  }

  @Test
  public void shouldUseOverriddenFlag() {
    // When the flag is overridden
    flags.overrides().add(IDE_SENTRY_ENABLED, false);

    // Then do something that will evaluate the flag, and verify the behavior of that something
    assertFalse(exampleBean.publishToSentry());
  }

  /**
   * This example is just here to show how you might use {@link FeatureFlags} in a bean and then
   * create a test with feature flag usage/overrides.
   */
  @ApplicationScoped
  public static class ExampleBean {

    @Inject
    FeatureFlags flags;

    public boolean publishToSentry() {
      if (flags.evaluateAsBoolean(IDE_SENTRY_ENABLED)) {
        Log.info("Doing something in the example bean based on Sentry usage");
        return true;
      }

      return false;
    }
  }
}
