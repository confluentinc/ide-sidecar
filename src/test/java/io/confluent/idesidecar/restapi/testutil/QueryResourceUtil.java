package io.confluent.idesidecar.restapi.testutil;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asGraphQLRequest;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import java.util.function.Function;

public class QueryResourceUtil {

  public static ValidatableResponse queryGraphQLRaw(String query) {
    String jsonString = asGraphQLRequest(query);
    return RestAssured.given()
        .contentType(ContentType.JSON)
        .body(jsonString)
        .when()
        .post("/gateway/v1/graphql")
        .then()
        .assertThat()
        .statusCode(200);
  }

  public static void assertQueryResponseMatches(
      String queryResourcePath,
      String expectedJsonResourcePath,
      Function<String, String> expectedResponseModifier
  ) {
    var expectedContent = loadResource(expectedJsonResourcePath);
    if (expectedResponseModifier != null) {
      expectedContent = expectedResponseModifier.apply(expectedContent);
    }
    JsonNode expected = asJson(expectedContent);

    String response = queryGraphQLRaw(
        loadResource(queryResourcePath)
    ).and().extract().body().asString();

    // Verify the output matches
    JsonNode actual = asJson(response);
    assertEquals(expected, actual);
  }

  public static void assertQueryResponseMatches(
      String queryResourcePath,
      String expectedJsonResourcePath
  ) {
    assertQueryResponseMatches(
        queryResourcePath,
        expectedJsonResourcePath,
        Function.identity()
    );
  }
}
