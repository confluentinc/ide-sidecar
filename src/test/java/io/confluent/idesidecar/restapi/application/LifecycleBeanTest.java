package io.confluent.idesidecar.restapi.application;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.models.SidecarAccessToken;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(BrokenTemplatesProfile.class)
class LifecycleBeanTest {

  @Test
  void testBadTemplatesZipDoesNotBreakApp() {
    // The app should start up even if the templates zip resource is malformed.
    // The registry service should be created, but empty -- no templates should be loaded.

    var sidecarAccessToken = given()
        .when().get("/gateway/v1/handshake")
        .then()
        .statusCode(200)
        .extract().body().as(SidecarAccessToken.class);

    given()
        .when()
        .header("Authorization", "Bearer " + sidecarAccessToken.authSecret())
        .get("/gateway/v1/templates")
        .then()
        .statusCode(200)
        .body("data.size()", equalTo(0));

    given()
        .when()
        .header("Authorization", "Bearer " + sidecarAccessToken.authSecret())
        .get("/gateway/v1/templates/foobar")
        .then()
        .statusCode(404);
  }
}