package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import java.io.IOException;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
public class LoginRealmResourceTest {

  WireMock wireMock;

  @BeforeEach
  void registerWireMockRoutes() throws IOException {
    wireMock.register(
        WireMock
            .get("/api/login/realm")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        new String(Objects.requireNonNull(
                            Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(
                                    "ccloud-oauth-mock-responses/login-realm.json")
                        ).readAllBytes()))).atPriority(100));
  }

  @AfterEach
  void resetWireMock() {
    wireMock.removeMappings();
  }

  @Test
  void proxyLoginRealmRequestShouldProxyStatusCodeAndResponseBody() throws IOException {
    var expectedResponseBody = new String(
        Thread.currentThread().getContextClassLoader().getResourceAsStream(
            "ccloud-oauth-mock-responses/login-realm.json").readAllBytes());

    var actualResponseBody = given()
        .when()
        .get("/api/login/realm")
        .then()
        .statusCode(200)
        .header("Content-Type", "application/json")
        .extract().body().asString();

    Assertions.assertEquals(expectedResponseBody, actualResponseBody);

    expectedResponseBody = "Whoops - Something got wrong";
    var expectedStatusCode = 500;
    wireMock.register(
        WireMock
            .get("/api/login/realm")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(expectedStatusCode)
                    .withHeader("Content-Type", "text/plain")
                    .withBody(expectedResponseBody)).atPriority(50));

    actualResponseBody = given()
        .when()
        .get("/api/login/realm")
        .then()
        .statusCode(expectedStatusCode)
        .header("Content-Type", "text/plain")
        .extract().body().asString();

    Assertions.assertEquals(expectedResponseBody, actualResponseBody);
  }
}
