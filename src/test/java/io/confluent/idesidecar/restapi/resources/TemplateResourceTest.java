package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for the TemplateResource class.
 */
@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@TestHTTPEndpoint(TemplateResource.class)
@ConnectWireMock
public class TemplateResourceTest {
  @InjectMock
  UuidFactory uuidFactory;

  WireMock wireMock;

  @BeforeEach
  void setup() {
    // Set up common mocks
    Mockito.when(uuidFactory.getRandomUuid()).thenReturn("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4");
  }

  @Test
  void listShouldReturnAllTemplates() {
    wireMock.register(
        WireMock
            .get("/scaffold/v1/template-collections/vscode/templates")
            .willReturn(WireMock.aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody(loadResource("ccloud-scaffolding-service-mocks/list-templates-response.json")))
    );

    given()
        .when()
        .get()
        .then()
        .statusCode(200)
        .body("data.size()", greaterThan(0))
        .body("metadata.total_size", greaterThan(0));
  }

  @Test
  void getShouldReturnTemplate() {
    wireMock.register(
        WireMock
            .get("/scaffold/v1/template-collections/vscode/templates/go-consumer")
            .willReturn(WireMock.aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody(loadResource("ccloud-scaffolding-service-mocks/get-template-response.json")))
    );

    given()
        .when()
        .get("go-consumer")
        .then()
        .statusCode(200)
        .body("spec.name", equalTo("go-consumer"));
  }

  @Test
  void getShouldReturnErrorWhenAccessingNonexistentTemplate() {
    wireMock.register(
        WireMock
            .get("/scaffold/v1/template-collections/vscode/templates/nonexistent-template")
            .willReturn(WireMock.aResponse()
                .withStatus(404)
                .withHeader("Content-Type", "application/json")
                .withBody(loadResource("ccloud-scaffolding-service-mocks/template-not-found-response.json")))
    );

    given()
        .when()
        .get("nonexistent-template")
        .then()
        .statusCode(404);
  }

  @Test
  void applyShouldReturnSuccess() {
    wireMock.register(
        WireMock
            .post("/scaffold/v1/template-collections/vscode/templates/go-consumer/apply")
            .willReturn(WireMock.aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/zip")
                .withBody(loadResource("ccloud-scaffolding-service-mocks/apply-go-consumer-response.zip")))
    );

    given()
        .when()
        .header("Content-Type", "application/json")
        .body(loadResource("ccloud-scaffolding-service-mocks/apply-go-consumer-request.json"))
        .post("go-consumer/apply")
        .then()
        .statusCode(200);
  }

  @Test
  void applyTemplateWithBadOptionsShouldReturnError() {
    wireMock.register(
        WireMock
            .post("/scaffold/v1/template-collections/vscode/templates/go-consumer/apply")
            .withRequestBody(
                WireMock.equalToJson(loadResource("ccloud-scaffolding-service-mocks/apply-template-bad-options-request.json"))
            )
            .willReturn(WireMock.aResponse()
                .withStatus(422)
                .withHeader("Content-Type", "application/json")
                .withBody(loadResource("ccloud-scaffolding-service-mocks/apply-template-bad-options-response.json"))
            )
    );

    given()
        .when()
        .header("Content-Type", "application/json")
        .body(loadResource("ccloud-scaffolding-service-mocks/apply-template-bad-options-request.json"))
        .post("go-consumer/apply")
        .then()
        .statusCode(422);
  }
}
