package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
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
        new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD));

    String authorizationCode = "bar";
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(authorizationCode);

    given()
        .queryParam("code", authorizationCode)
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Complete"));
  }

  @Test
  void callbackRendersAuthenticationFailedIfAuthenticationWasNotSuccessful()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.CCLOUD));

    wireMock.register(
        WireMock
            .post("/oauth/token")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody("")).atPriority(50));

    given()
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"));
  }

  @Test
  void callbackRendersAuthenticationFailedIfConnectionIsNotOfTypeCCloud()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.LOCAL));

    given()
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"));
  }

  @Test
  void callbackRendersAuthenticationFailedIfStateIsInvalid()
      throws CreateConnectionException {

    var connectionState = connectionStateManager.createConnectionState(
        new ConnectionSpec("id-1", "conn-1", ConnectionType.LOCAL));

    given()
        .queryParam("state", "INVALID_STATE")
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Failed"))
        .body(containsString("Invalid or expired state INVALID_STATE"));
  }
}
