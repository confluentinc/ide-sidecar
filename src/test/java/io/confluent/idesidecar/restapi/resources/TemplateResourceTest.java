package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResourceAsBytes;
import static io.confluent.idesidecar.scaffolding.util.PortablePathUtil.portablePath;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for the TemplateResource class.
 *<p>
 * The src/test/resources/static/templates/ directory are zipped to create
 * the templates.zip file in the src/test/resources/static directory. This is done
 * automatically in the generate-test-resources phase of the build. This file
 * is gitignored, so you don't have to worry about it being checked in.
 * </p>
 */
@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@TestHTTPEndpoint(TemplateResource.class)
public class TemplateResourceTest {
  @InjectMock
  UuidFactory uuidFactory;

  @BeforeEach
  void setup() {
    // Set up common mocks
    Mockito.when(uuidFactory.getRandomUuid()).thenReturn("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4");
  }

  @Test
  void listShouldReturnAllTemplates() {
    var expectedResponse = asJson(
        loadResource("templates-api-responses/list-templates-response.json")
    );

    var response = given()
        .when().get()
        .then().statusCode(200)
        .extract().body().asString();

    var actual = asJson(response);
    assertEquals(expectedResponse, actual);
  }

  @Test
  void getShouldReturnTemplate() {
    var expectedResponse = asJson(
        loadResource("templates-api-responses/get-template-response.json")
    );

    var response = given()
        .when().get("go-consumer")
        .then().statusCode(200)
        .extract().body().asString();

    var actual = asJson(response);
    assertEquals(expectedResponse, actual);
  }

  @Test
  void getShouldReturnErrorWhenAccessingNonexistentTemplate() {
    var expectedResponse = asJson(
        loadResource("templates-api-responses/get-nonexistent-template-response.json")
    );

    var response = given()
        .when().get("nonexistent-template")
        .then().statusCode(404)
        .extract().body().asString();

    var actual = asJson(response);
    assertEquals(expectedResponse, actual);
  }

  @QuarkusTest
  @Nested
  @TestHTTPEndpoint(TemplateResource.class)
  class ApplyTemplateTests {

    @Test
    void applyTemplateShouldReturnZipFile() throws IOException {
      // `auto_offset_reset` is provided and has a default value
      // `cc_schema_registry_url` is not provided but has a default value
      var requestBodyString = """
          {
            "options": {
              "api_key": "fake-api-key",
              "api_secret": "fake-api-secret",
              "auto_offset_reset": "earliest-overridden",
              "cc_bootstrap_server": "fake-bootstrap-server",
              "cc_topic": "fake-topic",
              "group_id": "fake-group-id",
              "include_producer": true,
              "TeSt_WiTh_MiXeD_CaSe": "ThIs Is MiXeD cAsE",
              "TEST_UPPERCASE": "I'M NOT SHOUTING",
              "cc_schema_registry_url": "http://localhost:8081",
              "sample_list": ["item1", "item2", "item3", "item4"],
              "app_name": "go_consumer_from_api"
            }
          }
          """;

      // Make the API call to create a go-consumer
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("go-consumer/apply")
          .then().statusCode(200)
          .contentType("application/octet-stream")
          // Unzip the response and compare the contents
          .extract().body().asByteArray();

      var contentMap = unzip(response);
      // -- Assert that the list of files in the zip file is as expected
      // Compare Paths not strings to avoid platform-specific issues
      assertEquals(Set.of(
          // From src/ directory
          portablePath("config.properties"),
          portablePath("go_consumer_from_api.md"),
          portablePath("deeply", "nested", "folder", "nested_file"),
          // These must come from static/ directory, contents will be asserted
          // later in this test
          portablePath(".gitignore"),
          portablePath("README.md"),
          portablePath("deeply", "nested", "folder", "nested_file_2")
      ), contentMap.keySet());

      // -- Assert contents of each file
      // config.properties
      assertEquals("""
          bootstrap.servers=fake-bootstrap-server
          security.protocol=SASL_SSL
          sasl.mechanisms=PLAIN
          sasl.username=fake-api-key
          sasl.password=fake-api-secret

          auto.offset.reset=earliest-overridden
          group.id=fake-group-id

          # Positive test cases
          # fake-api-key
          # fake-bootstrap-server
          # earliest-overridden
          # fake-group-id
          # http://localhost:8081
          # fake-topic
          # Use triple mustaches to return raw values without escaping
          # I'M NOT SHOUTING
          # ThIs Is MiXeD cAsE

          producer.config1=producer_value1
          producer.config2=producer_value2

          item1
          item2
          item3
          item4

          # Neutral test cases
          # { must_remain_braced }
          # \\{\\{ must_remain_braced_five \\}\\}
          # I'm just a regular old string
          # Placeholders without replacements will be removed
          #\s
          #\s
          #\s
          #\s
          """, contentMap.get("config.properties"));

      // consumer_go
      assertEquals("""
          # This file must be named go_consumer_from_api.md

          topic := "fake-topic"
          schema_registry_url := "http://localhost:8081"
          """, contentMap.get("go_consumer_from_api.md"));

      // deeply/nested/folder/nested_file
      assertEquals("""
          Hi! I'm a nested file. There's nothing to see here. Move along. Move along.

          Ok, fine, here's a joke: Why did the tomato turn red? Because it saw the salad dressing.

          Oh, also a test for the api_key: fake-api-key. That's all, goodbye!
          """, contentMap.get(portablePath("deeply/nested/folder/nested_file")));

      // .gitignore
      assertEquals("""
          config.properties
          """, contentMap.get(".gitignore"));

      // README.md
      assertEquals("""
          I am static/README.md. Good has prevailed over src/README.md
          """, contentMap.get("README.md"));

      // deeply/nested/folder/nested_file_2
      assertEquals("""
          I will always win. I have the power of static directories on my side.
          """, contentMap.get(portablePath("deeply/nested/folder/nested_file_2")));
    }

    @Test
    void applyTemplateShouldWorkWithoutStaticDirectory() throws IOException {
      var requestBodyString = """
            {
              "options": {
                "api_key": "fake-api-key",
                "api_secret": "fake-api-secret",
                "topic": "fake-topic",
                "group_id": "fake-group-id",
                "auto_commit_offsets": "true"
              }
            }
          """;
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("template-without-static-dir/apply")
          .then().statusCode(200)
          .contentType("application/octet-stream")
          // Unzip the response and compare the contents
          .extract().body().asByteArray();

      var contentMap = unzip(response);

      // -- Assert that the list of files in the zip file is as expected
      assertEquals(Set.of(portablePath("README.md")), contentMap.keySet());
    }

    @Test
    void applyTemplateDoesNotRenderStaticFiles() throws IOException {
      var requestBodyString = """
            {
              "options": {}
            }
          """;
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("template-with-executable-in-static-dir/apply")
          .then().statusCode(200)
          .contentType("application/octet-stream")
          .extract().body().asByteArray();

      var contentMap = unzipAsBytes(response);

      // -- Assert that the list of files in the zip file is as expected
      assertEquals(Set.of("do_not_parse.md", "README.md", "executable_binary"), contentMap.keySet());

      // Assert that the byte contents are identical to the original file
      var expectedExecutableBytes = loadResourceAsBytes(
          "static/templates/template-with-executable-in-static-dir/static/executable_binary");

      assertArrayEquals(expectedExecutableBytes, contentMap.get("executable_binary"));

      // Assert that do_not_parse.md is not rendered
      assertEquals("""
          Please do not {{parse}} me!
          """, new String(contentMap.get("do_not_parse.md")));
    }

    @Test
    void applyTemplateShouldAllowDynamicFilePaths() throws IOException {
      var requestBodyString = """
              {
                "options": {
                  "app_name": "MyAwesomeApp",
                  "package_name": "com.foo.bar.baz",
                  "package_path": "src/main/java/com/foo/bar/baz"
                }
              }
          """;

      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("java-consumer/apply")
          .then().statusCode(200)
          .contentType("application/octet-stream")
          .extract().body().asByteArray();

      var contentMap = unzip(response);

      assertEquals(Set.of(
          portablePath("src/main/java/com/foo/bar/baz/MyAwesomeApp.java")
      ), contentMap.keySet());

      assertEquals("""
          package com.foo.bar.baz;

          public class MyAwesomeApp {
              public static void main(String[] args) {
                  System.out.println("Hello, World!");
              }
          }
          """, contentMap.get(
              portablePath("src/main/java/com/foo/bar/baz/MyAwesomeApp.java")));
    }

    @Test
    void applyTemplateShouldReturnErrorWhenAccessingNonexistentTemplate() {
      var expectedResponse = asJson(
          loadResource("templates-api-responses/apply-template/nonexistent-template-response.json")
      );

      var requestBodyString = """
          {
            "options": {}
          }
          """;
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("nonexistent-template/apply")
          .then().statusCode(404)
          // Ensure we don't send zip upon error
          .contentType("application/json")
          .extract().body().asString();

      var actual = asJson(response);
      assertEquals(expectedResponse, actual);
    }

    @Test
    void applyTemplateShouldReturnErrorWhenUnsupportedOptionsAreProvided() {
      var expectedResponse = asJson(
          loadResource("templates-api-responses/apply-template/unsupported-options-response.json")
      );

      var requestBodyString = """
          {
            "options": {
              "api_key": "fake-api-key",
              "api_secret": "fake-api-secret",
              "cc_bootstrap_server": "fake-bootstrap-server",
              "cc_topic": "fake-topic",
              "group_id": "fake-group-id",
              "unsupported_option": "unsupported-value",
              "foo": "spam",
              "auto_offset_reset": "earliest",
              "cc_schema_registry_url": "http://localhost:8081",
              "TEST_UPPERCASE": "",
              "TeSt_WiTh_MiXeD_CaSe": "",
              "include_producer": "false",
              "sample_list": ["foo"],
              "app_name": "go_consumer"
            }
          }
          """;

      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("go-consumer/apply")
          .then().statusCode(400)
          .contentType("application/json")
          .extract().body().asString();

      var actual = asJson(response);
      assertEquals(expectedResponse, actual);
    }


    @Test
    void applyTemplateShouldReturnErrorWhenOptionsAreMissing() {
      var expectedResponse = asJson(
          loadResource("templates-api-responses/apply-template/missing-options-response.json")
      );

      // Missing `group_id` option
      var requestBodyString = """
          {
            "options": {
              "api_key": "fake-api-key",
              "api_secret": "fake-api-secret",
              "cc_bootstrap_server": "fake-bootstrap-server",
              "cc_topic": "fake-topic"
            }
          }
          """;
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("go-consumer/apply")
          .then().statusCode(400)
          .contentType("application/json")
          .extract().body().asString();

      // Use Junit assertEquals instead of RestAssured's body(is()) to
      // get the diff output in case of a failure
      var actual = asJson(response);
      assertEquals(expectedResponse, actual);
    }

    @Test
    void applyTemplateShouldReturnErrorWhenOptionViolatesMinLengthConstraint() {
      var expectedResponse = asJson(
          loadResource("templates-api-responses/apply-template/option-violates-min-length-constraint-response.json")
      );

      var requestBodyString = """
          {
            "options": {
              "app_name": "fast-java-app",
              "package_path": "io.confluent.dtx",
              "package_name": ""
            }
          }
          """;
      var response = given()
          .body(requestBodyString)
          .contentType("application/json")
          .when().post("java-consumer/apply")
          .then().statusCode(400)
          .contentType("application/json")
          .extract().body().asString();

      // Use Junit assertEquals instead of RestAssured's body(is()) to
      // get the diff output in case of a failure
      var actual = asJson(response);
      assertEquals(expectedResponse, actual);
    }

    private static Map<String, String> unzip(byte[] zippedContents) throws IOException {
      return unzipAsBytes(zippedContents).entrySet().stream()
          .collect(
              HashMap::new,
              (map, entry) -> map.put(
                  portablePath(entry.getKey()),
                  new String(entry.getValue(), Charset.defaultCharset())
              ),
              HashMap::putAll
          );
    }

    private static Map<String, byte[]> unzipAsBytes(byte[] zippedContents) throws IOException {
      Map<String, byte[]> contentsMap = new HashMap<>();

      try (ZipInputStream zipInputStream = new ZipInputStream(
          new ByteArrayInputStream(zippedContents))) {
        ZipEntry entry;
        while ((entry = zipInputStream.getNextEntry()) != null) {
          String entryName = entry.getName();
          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
          byte[] buffer = new byte[1024];
          int length;
          while ((length = zipInputStream.read(buffer)) > 0) {
            byteArrayOutputStream.write(buffer, 0, length);
          }
          contentsMap.put(entryName, byteArrayOutputStream.toByteArray());
          zipInputStream.closeEntry();
        }
      }
      return contentsMap;
    }
  }
}
