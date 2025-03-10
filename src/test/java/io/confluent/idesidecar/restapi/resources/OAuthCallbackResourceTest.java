package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.not;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.confluent.idesidecar.restapi.util.VsCodeExtensionUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@ConnectWireMock
public class OAuthCallbackResourceTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  @BeforeEach
  void registerWireMockRoutes() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
  }

  @AfterEach
  void resetWireMock() {
    wireMock.removeMappings();
    connectionStateManager.clearAllConnectionStates();
  }

  @Test
  void callbackRendersAuthenticationCompleteIfAuthenticationWasSuccessful()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD)
    );

    String authorizationCode = "bar";
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(authorizationCode);

    verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(connectionState.getId());

    given()
        .queryParam("code", authorizationCode)
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Complete"))
        .body(containsString(VsCodeExtensionUtil.CCLOUD_OAUTH_VSCODE_EXTENSION_URI));

    // CCloud connection should hold valid control plane token and have no errors
    verifyStateAndErrorsOfConfluentCloudConnection(
        connectionState.getId(),
        ConnectedState.SUCCESS,
        false
    );
  }

  @Test
  void callbackSupportsConnectionSpecificRedirectUri() throws CreateConnectionException {
    var customRedirectUri = "vscode://custom-redirect-uri";
    var connectionSpec = new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD);
    var connectionState = connectionStateManager.createConnectionState(
        connectionSpec.withCCloudConfig(new CCloudConfig(null, customRedirectUri))
    );

    String authorizationCode = "bar";
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(authorizationCode);

    verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(connectionState.getId());

    given()
        .queryParam("code", authorizationCode)
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Complete"))
        .body(not(containsString(VsCodeExtensionUtil.CCLOUD_OAUTH_VSCODE_EXTENSION_URI)))
        .body(containsString(customRedirectUri));

    // CCloud connection should hold valid control plane token and have no errors
    verifyStateAndErrorsOfConfluentCloudConnection(
        connectionState.getId(),
        ConnectedState.SUCCESS,
        false
    );
  }

  @Test
  void callbackRendersAuthenticationFailedIfAuthenticationWasNotSuccessful()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD)
    );

    wireMock.register(
        WireMock
            .post("/oauth/token")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("")
            ).atPriority(50)
    );

    verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(connectionState.getId());

    given()
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"));

    // CCloud connection should remain in initial state but have errors
    verifyStateAndErrorsOfConfluentCloudConnection(
        connectionState.getId(),
        ConnectedState.NONE,
        true
    );
  }

  @Test
  void callbackRendersAuthenticationFailedIfConnectionIsNotOfTypeCCloud()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.LOCAL)
    );

    given()
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"));
  }

  @Test
  void callbackRendersAuthenticationFailedIfStateIsInvalid() throws CreateConnectionException {
    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD)
    );

    verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(connectionState.getId());

    given()
        .queryParam("state", "INVALID_STATE")
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"))
        .body(containsString("Invalid or expired state INVALID_STATE"));

    // CCloud connection should remain in initial state and have no errors
    verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(connectionState.getId());
  }

  void verifyStatusOfConfluentCloudConnectionEqualsInitialStatus(String connectionId) {
    verifyStateAndErrorsOfConfluentCloudConnection(
        connectionId,
        ConnectedState.NONE,
        false
    );
  }

  /**
   * Helper method to verify if the fields <code>state</code> and <code>errors</code> of a given
   * CCloud connection's status match given expectations.
   *
   * @param connectionId  the ID of the CCloud connection
   * @param expectedState the expected state of the CCloud connection
   * @param expectErrors  whether we expect the CCloud connection to hold errors
   */
  void verifyStateAndErrorsOfConfluentCloudConnection(
      String connectionId,
      ConnectedState expectedState,
      boolean expectErrors
  ) {
    given()
        .when()
        .get("/gateway/v1/connections/%s".formatted(connectionId))
        .then()
        .statusCode(200)
        .body("status.ccloud.state", equalTo(expectedState.toString()))
        .body("status.ccloud.errors", expectErrors ? notNullValue() : nullValue());
  }
}
