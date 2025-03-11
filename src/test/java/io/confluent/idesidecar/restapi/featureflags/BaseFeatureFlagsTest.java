package io.confluent.idesidecar.restapi.featureflags;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.launchdarkly.sdk.LDContext;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class BaseFeatureFlagsTest {

  static final String PROJECT_NAME = "test-project";
  static final String CLIENT_ID = "client-1234567890";
  static final LDContext CONTEXT = LDContext.create("u-1234");
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final String LAUNCH_DARKLY_RESPONSE_RESOURCE_PATH = "featureflags/launch-darkly-evaluation-response.json";
  static final String LAUNCH_DARKLY_ERROR_RESOURCE_PATH = "featureflags/launch-darkly-error-response.json";
  static final String LAUNCH_DARKLY_EMPTY_RESOURCE_PATH = "featureflags/launch-darkly-empty-response.json";
  static final String LAUNCH_DARKLY_NON_DEFAULT_RESOURCE_PATH = "featureflags/launch-darkly-non-default-response.json";

  Collection<FlagEvaluation> assertProjectRefresh(
      FeatureProject project,
      LDContext context,
      WebClientFactory webClientFactory
  ) {
    var latch = new CountDownLatch(1);

    project.refresh(context, webClientFactory, latch::countDown);
    await(latch);
    return project.evaluations().evaluations();
  }

  Collection<FlagEvaluation> assertProviderRefresh(
      FeatureProject.Provider provider,
      LDContext context,
      WebClientFactory webClientFactory
  ) {
    var latch = new CountDownLatch(1);
    var results = new AtomicReference<Collection<FlagEvaluation>>();

    provider.evaluateFlags(context, webClientFactory, evaluations -> {
      latch.countDown();
      results.set(evaluations);
    });
    await(latch);

    // Make a copy and deduplicate
    return results.get() != null ? Set.copyOf(results.get()) : null;
  }

  void assertEvaluationsMatch(Collection<FlagEvaluation> expected, FeatureProject project) {
    var actual = new HashSet<>(project.evaluations().evaluations());
    assertEquals(expected, actual);
  }

  static Set<FlagEvaluation> loadEvaluationsFromResourceFile(String resourcePath,
      String projectName) {
    return Set.copyOf(
        FlagEvaluations
            .loadFromClasspath(resourcePath, projectName, "test feature flags")
            .evaluations()
    );
  }

  public static void whenLaunchDarklyReturns(
      WireMock wireMock,
      UrlPattern urlPattern,
      int responseStatus,
      String responseJson
  ) {
    wireMock.register(
        WireMock
            .get(urlPattern)
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(responseStatus)
                    .withBody(responseJson)
            )
            .atPriority(100)
    );
  }

  public static void whenLaunchDarklyReturnsNoFlags(WireMock wireMock) {
    whenLaunchDarklyReturns(
        wireMock,
        urlPathForProject(FeatureFlags.IDE),
        200,
        loadResource(LAUNCH_DARKLY_EMPTY_RESOURCE_PATH)
    );
    whenLaunchDarklyReturns(
        wireMock,
        urlPathForProject(FeatureFlags.CCLOUD),
        200,
        loadResource(LAUNCH_DARKLY_EMPTY_RESOURCE_PATH)
    );
  }

  public static void whenLaunchDarklyReturnsNonDefaultIdeFlags(WireMock wireMock) {
    whenLaunchDarklyReturns(
        wireMock,
        urlPathForProject(FeatureFlags.IDE),
        200,
        loadResource(LAUNCH_DARKLY_NON_DEFAULT_RESOURCE_PATH)
    );
  }

  static UrlPattern urlPathForProject(FeatureProject project) {
    return urlPathMatching("/api/flags/" + project.clientId + "/contexts/.*");
  }

  public static void await(CountDownLatch latch) {
    try {
      assertTrue(
          latch.await(2, TimeUnit.SECONDS)
      );
    } catch (InterruptedException e) {
      fail("Interrupted while waiting for latch", e);
    }
  }
}
