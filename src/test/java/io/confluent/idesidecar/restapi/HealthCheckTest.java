package io.confluent.idesidecar.restapi;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
public class HealthCheckTest {

  @Test
  public void testHealthCheck() {
    given()
        .when()
        .get("/gateway/v1/health/ready")
        .then()
        .statusCode(200)
        .body("status", is("UP"));
  }
}