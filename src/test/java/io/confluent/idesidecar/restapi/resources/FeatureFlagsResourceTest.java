package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.featureflags.BaseFeatureFlagsTest;
import io.confluent.idesidecar.restapi.featureflags.FeatureFlagTestConstants;
import io.confluent.idesidecar.restapi.featureflags.FeatureFlags;
import io.confluent.idesidecar.restapi.featureflags.FlagId;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
class FeatureFlagsResourceTest extends BaseFeatureFlagsTest implements FeatureFlagTestConstants {

  @InjectMock
  SidecarInfo sidecar;

  @Inject
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
  }

  @AfterEach
  void cleanUp() {
    wireMock.removeMappings();
  }

  @Test
  void shouldGetFlagValues() {
    // Make sure to refresh the flags via WireMock
    whenLaunchDarklyReturnsNoFlags(wireMock);
    await(
        flags.refreshFlags(true)
    );

    // Then test getting the flags
    assertEquals(
        BooleanNode.getTrue(),
        getFlag(FlagId.IDE_GLOBAL_ENABLE)
    );
    assertEquals(
        BooleanNode.getTrue(),
        getFlag(FlagId.IDE_CCLOUD_ENABLE)
    );
    assertEquals(
        BooleanNode.getTrue(),
        getFlag(FlagId.IDE_SENTRY_ENABLED)
    );
    assertEquals(
        BooleanNode.getFalse(),
        getFlag(FlagId.IDE_SCAFFOLDING_SERVICE_ENABLED)
    );
    assertEquals(
        FeatureFlags.OBJECT_MAPPER.createArrayNode(),
        getFlag(FlagId.IDE_GLOBAL_NOTICES)
    );
  }

  static JsonNode getFlag(FlagId flagId) {
    // Submit the REST request
    var responseBody = given()
        .when()
        .get("/gateway/v1/feature-flags/%s/value".formatted(flagId.id()))
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();

    // Verify the response is as expected
    return asJson(responseBody);
  }
}