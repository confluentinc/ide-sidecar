package io.confluent.idesidecar.restapi.featureflags;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Set;
import java.util.function.Consumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
class FeatureProjectTest extends BaseFeatureFlagsTest {

  FeatureProject project;

  WireMock wireMock;

  @ConfigProperty(name = "ide-sidecar.feature-flags.ide-project.eval-uri")
  String httpUrl;

  @Inject
  WebClientFactory webClientFactory;

  @BeforeEach
  void setup() {
  }

  @AfterEach
  void cleanUp() {
    wireMock.removeMappings();
  }

  @Test
  public void shouldConfigureHttpProvider() {
    // When a provider is configured with a URL
    configureProjectWith(httpUrl);

    // Then the provider should be a HTTP provider
    assertProvider(
        HttpFlagEvaluationProvider.class,
        provider -> {
          assertEquals(PROJECT_NAME, provider.projectName);
          assertEquals(CLIENT_ID, provider.clientId);
          assertEquals(httpUrl, provider.fetchUri);
        }
    );

    // When evaluating the flags
    whenLaunchDarklyReturns(
        wireMock,
        urlPathMatching("/api/flags/.*"),
        200,
        loadResource(LAUNCH_DARKLY_RESPONSE_RESOURCE_PATH)
    );
    assertProjectRefresh(project, CONTEXT, webClientFactory);

    // Then the project's evaluations will match expected results
    var expected = loadEvaluationsFromResourceFile(
        LAUNCH_DARKLY_RESPONSE_RESOURCE_PATH,
        PROJECT_NAME
    );
    assertEvaluationsMatch(expected, project);


    // When evaluating the flags again and LD fails
    whenLaunchDarklyReturns(
        wireMock,
        urlPathMatching("/api/flags/.*"),
        400,
        loadResource(LAUNCH_DARKLY_ERROR_RESOURCE_PATH)
    );
    assertProjectRefresh(project, CONTEXT, webClientFactory);

    // Then the project's evaluations will unchanged
    assertEvaluationsMatch(expected, project);


    // When evaluating the flags again and LD returns empty
    whenLaunchDarklyReturns(
        wireMock,
        urlPathMatching("/api/flags/.*"),
        200,
        loadResource(LAUNCH_DARKLY_EMPTY_RESOURCE_PATH)
    );
    assertProjectRefresh(project, CONTEXT, webClientFactory);

    // Then the project's evaluations will be empty
    assertEvaluationsMatch(Set.of(), project);
  }

  void configureProjectWith(String uri) {
    Log.infof("Configuring test FeatureProject '%s' with URL: %s", PROJECT_NAME, httpUrl);
    project = new FeatureProject(PROJECT_NAME, CLIENT_ID, uri);
  }

  <ProviderT extends FeatureProject.Provider> void assertProvider(
      Class<ProviderT> providerType,
      Consumer<ProviderT> checker
  ) {
    // that initially has no flag evaluations
    assertEquals(0, project.evaluations().size());

    assertEquals(providerType, project.provider.getClass());
    ProviderT provider = providerType.cast(project.provider);
    if (checker != null) {
      checker.accept(provider);
    }
  }
}
