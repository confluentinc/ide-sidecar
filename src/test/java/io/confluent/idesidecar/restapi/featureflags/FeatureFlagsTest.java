package io.confluent.idesidecar.restapi.featureflags;

import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_CCLOUD_ENABLE;
import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_GLOBAL_ENABLE;
import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_GLOBAL_NOTICES;
import static io.confluent.idesidecar.restapi.featureflags.FlagId.IDE_SENTRY_ENABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.MockitoConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
class FeatureFlagsTest extends BaseFeatureFlagsTest implements FeatureFlagTestConstants {

  static final Set<FlagEvaluation> DEFAULT_FLAGS = Set.copyOf(
      FlagEvaluations.defaults().evaluations()
  );

  @InjectMock
  @MockitoConfig(convertScopes = true)
  SidecarInfo sidecar;

  FeatureFlags flags;

  WireMock wireMock;

  @BeforeEach
  void setup() {
    // Set up the sidecar info
    when(sidecar.osName()).thenReturn(OS_NAME);
    when(sidecar.osVersion()).thenReturn(OS_VERSION);
    when(sidecar.osType()).thenReturn(OS_TYPE);
    when(sidecar.vsCode()).thenReturn(Optional.of(VS_CODE));
    when(sidecar.version()).thenReturn(SIDECAR_VERSION);

    // Set up the feature flags but do not call startup(...)
    flags = new FeatureFlags();
    flags.sidecar = sidecar;
  }

  @AfterEach
  void cleanUp() {
    wireMock.removeMappings();
  }

  @Test
  void shouldHaveDefaultFlagsBeforeRefreshing() {
    // When startup has not yet been called
    // Then the default flags will exist
    assertDefaultFlagsMatch(DEFAULT_FLAGS);

    // And the flags will exactly match the defaults
    assertFlagsMatch(DEFAULT_FLAGS);
  }

  @Test
  void shouldHaveDefaultFlags() {
    // When there are flags returned
    whenLaunchDarklyReturnsNoFlags(wireMock);

    // When we initialize the feature flags and startup fetches the flags
    await(
        flags.startup(null)
    );

    // Then the default flags will exist
    assertDefaultFlagsMatch(DEFAULT_FLAGS);

    // And the flags will exactly match the defaults
    assertFlagsMatch(DEFAULT_FLAGS);
  }

  @Test
  void shouldHaveNonDefaultFlags() {
    whenLaunchDarklyReturnsNonDefaultIdeFlags(wireMock);

    // When we initialize the feature flags and startup fetches the flags
    await(
        flags.startup(null)
    );

    // Then the default flags will exist
    assertDefaultFlagsMatch(DEFAULT_FLAGS);

    // But some of the flags will be different
    assertFlag(IDE_GLOBAL_ENABLE)
        .hasValue(true)
        .hasDefaultValue(true);
    assertFlag(IDE_CCLOUD_ENABLE)
        .hasValue(false)
        .hasDefaultValue(true);
    assertFlag(IDE_SENTRY_ENABLED)
        .hasValue(false)
        .hasDefaultValue(true);
    assertFlag(IDE_GLOBAL_NOTICES)
        .hasDefaultValues(List.of())
        .hasValues(
            new Notice("First global message", "First suggestion"),
            new Notice("Second global message", "Second suggestion")
        );

    // Check these that don't have defaults (and thus would not have FlagId literals)
    assertFlag("ide.example.integer")
        .hasNoDefaultValue()
        .hasValue(100);
    assertFlag("ide.example.long")
        .hasNoDefaultValue()
        .hasValue(9876543210123L);
    assertFlag("ide.example.string")
        .hasNoDefaultValue()
        .hasValue("the-value");
    assertFlag("ide.does.not-exist")
        .hasNoDefaultValue()
        .hasNoValue();
  }

  @Test
  void shouldAllowLocalOverrides() {
    whenLaunchDarklyReturnsNonDefaultIdeFlags(wireMock);

    // When we initialize the feature flags and startup fetches the flags
    await(
        flags.startup(null)
    );

    // Then the default flags will exist
    assertDefaultFlagsMatch(DEFAULT_FLAGS);

    assertFlag(IDE_GLOBAL_ENABLE)
        .hasValue(true)
        .hasDefaultValue(true);
    assertFlag(IDE_CCLOUD_ENABLE)
        .hasValue(false)
        .hasDefaultValue(true);
    assertFlag(IDE_SENTRY_ENABLED)
        .hasValue(false)
        .hasDefaultValue(true);
    assertFlag(IDE_GLOBAL_NOTICES)
        .hasDefaultValues(List.of())
        .hasValues(
            new Notice("First global message", "First suggestion"),
            new Notice("Second global message", "Second suggestion")
        );

    // When tests override an existing flag
    flags.overrides().add(IDE_CCLOUD_ENABLE, true);
    // Then the flag will have the new value
    assertFlag(IDE_CCLOUD_ENABLE)
        .hasValue(true)
        .hasDefaultValue(true);

    // When tests remove an override
    flags.overrides().remove(IDE_CCLOUD_ENABLE);
    // Then the flag will have the original value (that is not the default)
    assertFlag(IDE_CCLOUD_ENABLE)
        .hasValue(false)
        .hasDefaultValue(true);

    // When tests override an existing flag with an Object
    flags.overrides().add(IDE_SENTRY_ENABLED, Boolean.TRUE);
    // Then the flag will have the new value
    assertFlag(IDE_SENTRY_ENABLED)
        .hasValue(true)
        .hasDefaultValue(true);

    // When tests override an existing flag with a JSON value
    var notice = new Notice("Replacement global message", "Replacement suggestion");
    flags.overrides().add(IDE_GLOBAL_NOTICES, List.of(notice));
    // Then the flag will have the new value
    assertFlag(IDE_GLOBAL_NOTICES)
        .hasDefaultValues(List.of())
        .hasValues(notice);

    // When the overrides are cleared
    flags.overrides().clear();
    // then the flags will be reset back to the original values
    assertFlag(IDE_GLOBAL_ENABLE)
        .hasValue(true)
        .hasDefaultValue(true);
    assertFlag(IDE_CCLOUD_ENABLE)
        .hasValue(false)
        .hasDefaultValue(true);
    assertFlag(IDE_GLOBAL_NOTICES)
        .hasDefaultValues(List.of())
        .hasValues(
            new Notice("First global message", "First suggestion"),
            new Notice("Second global message", "Second suggestion")
        );
  }

  @Test
  void shouldInstantiateScheduledRefreshPredicate() {
    var predicate = new ScheduledRefreshPredicate();
  }

  void assertDefaultFlagsMatch(Set<FlagEvaluation> expected) {
    MutableFlagEvaluations defaultEvals = assertInstanceOf(
        MutableFlagEvaluations.class,
        flags.defaults()
    );
    assertEquals(expected, Set.copyOf(defaultEvals.evaluations()));
  }

  void assertFlagsMatch(Set<FlagEvaluation> expected) {
    assertEquals(expected, Set.copyOf(flags.evaluationsById().values()));
  }

  void assertFlagMatches(String id, JsonNode expectedValue, JsonNode expectedDefaultValue) {
    var defaultValue = flags.defaults().evaluateAsJson(id);
    if (expectedValue != null) {
      assertTrue(defaultValue.isPresent());
      assertEquals(expectedValue, defaultValue.get());
    } else {
      assertFalse(defaultValue.isPresent());
    }

    var actual = flags.evaluateAsJson(id);
    if (expectedValue != null) {
      assertTrue(actual.isPresent());
      assertEquals(expectedValue, actual.get());
    } else {
      assertFalse(actual.isPresent());
    }
  }

  FlagMatcher assertFlag(FlagId id) {
    return new FlagMatcher(flags, id, id.toString());
  }

  FlagMatcher assertFlag(String id) {
    return new FlagMatcher(flags, null, id);
  }

  record FlagMatcher(
      FeatureFlags flags,
      FlagId flagId,
      String id
  ) {

    FlagMatcher hasValue(boolean expected) {
      return hasValue(expected, flags::evaluateAsBoolean, BooleanNode.valueOf(expected));
    }

    FlagMatcher hasValue(int expected) {
      return hasValue(expected, flags::evaluateAsInteger, IntNode.valueOf(expected));
    }

    FlagMatcher hasValue(long expected) {
      return hasValue(expected, flags::evaluateAsLong, LongNode.valueOf(expected));
    }

    FlagMatcher hasValue(String expected) {
      return hasValue(expected, flags::evaluateAsString, new TextNode(expected));
    }

    <T> FlagMatcher hasValue(
        T expectedValue,
        Function<FlagId, T> typeEvaluator,
        JsonNode expectedJsonValue
    ) {
      if (flagId != null) {
        assertEquals(expectedValue, typeEvaluator.apply(flagId));
      }
      assertEquals(expectedJsonValue, flags.evaluateAsJson(id).get());
      return this;
    }

    <T> FlagMatcher hasValues(Class<T> type, List<T> expected) {
      if (flagId != null) {
        assertEquals(expected, flags.evaluateAsList(flagId, type));
      }
      JsonNode expectedJsonValue = MAPPER.valueToTree(expected);
      assertEquals(expectedJsonValue, flags.evaluateAsJson(id).get());
      return this;
    }

    FlagMatcher hasValues(Notice... expected) {
      return hasValues(Notice.class, Arrays.asList(expected));
    }

    FlagMatcher hasNoValue() {
      assertTrue(flags.evaluateAsJson(id).isEmpty());
      return this;
    }

    FlagMatcher hasNoDefaultValue() {
      assertTrue(flags.defaults().evaluateAsJson(id).isEmpty());
      return this;
    }

    FlagMatcher hasDefaultValue(boolean expected) {
      return hasDefaultValue(BooleanNode.valueOf(expected));
    }

    FlagMatcher hasDefaultValue(int expected) {
      return hasDefaultValue(IntNode.valueOf(expected));
    }

    FlagMatcher hasDefaultValue(long expected) {
      return hasDefaultValue(LongNode.valueOf(expected));
    }

    FlagMatcher hasDefaultValue(String expected) {
      return hasDefaultValue(new TextNode(expected));
    }

    FlagMatcher hasDefaultValue(JsonNode expectedValue) {
      assertEquals(expectedValue, flags.defaults().evaluateAsJson(id).get());
      return this;
    }

    <T> FlagMatcher hasDefaultValues(List<T> expected) {
      JsonNode expectedJsonValue = MAPPER.valueToTree(expected);
      assertEquals(expectedJsonValue, flags.defaults().evaluateAsJson(id).get());
      return this;
    }
  }
}
