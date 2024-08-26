package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.wildfly.common.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.models.ApplyTemplateRequest;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.security.SecureRandom;
import java.util.HashMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class TemplateResourceIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final int RANDOM_STRING_LENGTH = 25;

  @Test
  void shouldBeAbleToApplyAllTemplates() throws JsonProcessingException {
    // Get all templates
    var responseBody = given()
        .when()
        .get("/gateway/v1/templates")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);
    var templates = responseJson.get("data");

    // The field "data" should be an array ...
    assertTrue(templates.isArray());
    // ... and hold at least one template
    assertTrue(templates.elements().hasNext());

    // Verify that we can apply each template
    for (var template : templates) {
      var spec = OBJECT_MAPPER.readValue(
          template.get("spec").toString(),
          TemplateManifest.class);
      var name = spec.name();
      var payload = OBJECT_MAPPER.writeValueAsString(templateRequestWithTestData(spec));

      var statusCode = given()
          .when()
          .header("Content-Type", "application/json")
          .body(payload)
          .post("/gateway/v1/templates/%s/apply".formatted(name))
          .then()
          .extract()
          .statusCode();

      assertEquals(
          200,
          statusCode,
          "Observed unexpected status code when applying template (name=%s, payload=%s)."
              .formatted(name, payload));
    }
  }

  /**
   * Create the request payload for applying a provided template. Fill options with their default
   * value. If not available, fill options with one of their enums. If neither a default value nor
   * an enum are available, fill options with a random string.
   *
   * @param manifest the manifest of the template that should be applied
   * @return the request payload
   */
  ApplyTemplateRequest templateRequestWithTestData(TemplateManifest manifest) {
    var options = new HashMap<String, Object>();

    for (var option : manifest.options().entrySet()) {
      var name = option.getKey();
      var properties = option.getValue();

      if (properties.defaultValue() != null) {
        options.put(name, properties.defaultValue());
      } else if (properties.enums() != null && !properties.enums().isEmpty()) {
        options.put(name, properties.enums().getFirst());
      } else {
        options.put(name, generateRandomAlphabeticString(RANDOM_STRING_LENGTH));
      }
    }

    return new ApplyTemplateRequest(options);
  }

  /**
   * Create a random alphabetic string containing only characters between a and z (inclusive).
   *
   * @param length the length of the random string
   * @return the random string
   */
  String generateRandomAlphabeticString(int length) {
    return SECURE_RANDOM
        .ints('a', 'z' + 1)
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }
}
