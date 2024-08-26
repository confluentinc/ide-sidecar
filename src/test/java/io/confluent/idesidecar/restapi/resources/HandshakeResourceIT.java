package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
public class HandshakeResourceIT {
  @Test
  void shouldBeAbleToPerformHandshake() {
    // Should return access token when called for the first time
    var accessToken = given()
        .when()
        .get("/gateway/v1/handshake")
        .then().statusCode(200)
        .extract().jsonPath().getString("auth_secret");
    assertNotNull(accessToken);

    // Should return 401 for subsequent calls
    given()
        .when()
        .get("/gateway/v1/handshake")
        .then()
        .statusCode(401);

    // Calling a protected endpoint without the access token leads to a failure
    given()
        .when()
        .get("/gateway/v1/health/ready")
        .then()
        .statusCode(401);

    // Calling a protected endpoint with the access token works
    given()
        .when()
        .header("Authorization", "Bearer %s".formatted(accessToken))
        .get("/gateway/v1/health/ready")
        .then()
        .statusCode(200);
  }
}
