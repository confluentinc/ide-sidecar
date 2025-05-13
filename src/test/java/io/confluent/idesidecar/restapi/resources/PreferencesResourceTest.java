package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.credentials.KerberosCredentials;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.inject.Inject;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@TestHTTPEndpoint(PreferencesResource.class)
@TestMethodOrder(OrderAnnotation.class)
public class PreferencesResourceTest {

  @Inject
  WebClientFactory webClientFactory;

  @Test
  @Order(1)
  void getPreferencesShouldReturnAnEmptySpecByDefault() {
    var expectedResponse = asJson(
        loadResource("preferences/get-preferences-initial-response.json")
    );

    var responseBody = given()
        .when()
        .get()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertEquals(expectedResponse, responseJson);
  }

  @Test
  @Order(2)
  void updatePreferencesShouldAllowToUpdateSinglePreferences() {
    var expectedResponse = asJson(
        loadResource("preferences/update-single-preference-response.json")
    );

    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "trust_all_certificates": true
                  }
                }
                """
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertEquals(expectedResponse, responseJson);
  }

  @Test
  @Order(3)
  void updatePreferencesShouldUpdateTheConfigOfTheWebClient() {
    var expectedResponse = loadResource("preferences/update-multiple-preferences-response.json");

    // Verify that the web client does not trust any custom cert by default
    var trustedCerts = getTrustedCertsOfWebClient(webClientFactory.getWebClient());
    assertTrue(trustedCerts.isEmpty());

    var certPath = getCertPath("certs/artificial-rootca.pem");
    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "trust_all_certificates": true,
                    "tls_pem_paths": ["%s"]
                  }
                }
                """.formatted(certPath)
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    var expectedResponseJson = asJson(expectedResponse.formatted(certPath));
    assertEquals(expectedResponseJson, responseJson);

    // Verify that the web client trusts the provided PEM file
    trustedCerts = getTrustedCertsOfWebClient(webClientFactory.getWebClient());
    assertTrue(trustedCerts.contains(certPath));
  }

  @Test
  @Order(4)
  void updatePreferencesShouldReplaceNullValuesWithDefaults() {
    // Make sure that we've defined preferences for `trust_all_certificates` and `tls_pem_path`
    var expectedResponse = loadResource("preferences/update-multiple-preferences-response.json")
        .formatted(getCertPath("certs/artificial-rootca.pem"));
    var expectedResponseJson = asJson(expectedResponse);

    var responseBody = given()
        .when()
        .get()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertEquals(expectedResponseJson, responseJson);

    // Reset the preferences
    var expectedResponseWithDefaultValues = asJson(
        loadResource("preferences/get-preferences-initial-response.json")
    );

    responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "kerberos_config_file_path": null,
                    "tls_pem_paths": null,
                    "trust_all_certificates": null
                  }
                }
                """
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    responseJson = asJson(responseBody);

    assertEquals(expectedResponseWithDefaultValues, responseJson);
  }

  @Test
  @Order(5)
  void updatePreferencesShouldReturnErrorIfProvidedTlsPemPathDoesNotExist() {
    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "tls_pem_paths": ["cert-does-not-exist.pem"]
                  }
                }
                """
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(400)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertNotNull(responseJson);
    var errors = responseJson.get("errors");
    assertNotNull(errors);
    var error = errors.get(0);
    assertEquals(
        "The cert file 'cert-does-not-exist.pem' cannot be found.",
        error.get("detail").textValue()
    );
  }

  @Test
  @Order(6)
  void updatePreferencesShouldReturnErrorIfProvidedTlsPemPathIsEmpty() {
    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "tls_pem_paths": [""]
                  }
                }
                """
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(400)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertNotNull(responseJson);
    var errors = responseJson.get("errors");
    assertNotNull(errors);
    var error = errors.get(0);
    assertEquals("cert_path_empty", error.get("code").textValue());
    assertEquals("The cert file path cannot be null or empty.", error.get("detail").textValue());
  }

  @Test
  @Order(7)
  void updatePreferencesShouldReturnErrorIfProvidedKerberosConfigFilePathDoesNotExist() {
    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "kerberos_config_file_path": "does-not-exist.cfg"
                  }
                }
                """
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(400)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    assertNotNull(responseJson);
    var errors = responseJson.get("errors");
    assertNotNull(errors);
    var error = errors.get(0);
    assertEquals(
        "The Kerberos config file 'does-not-exist.cfg' cannot be found.",
        error.get("detail").textValue()
    );
  }

  @Test
  @Order(8)
  void updatePreferencesShouldUpdateTheKerberosSystemProperty() {
    // Make sure the system property is empty
    System.setProperty(KerberosCredentials.KERBEROS_CONFIG_FILE_PROPERTY_NAME, "");

    var configFilePath = Thread
        .currentThread()
        .getContextClassLoader()
        .getResource("credentials/empty-krb5-config.cfg")
        .getFile();
    var responseBody = given()
        .when()
        .body(
            """
                {
                  "api_version": "gateway/v1",
                  "kind": "Preferences",
                  "spec": {
                    "kerberos_config_file_path": "%s",
                    "tls_pem_paths": [],
                    "trust_all_certificates": false
                  }
                }
                """.formatted(configFilePath)
        )
        .header("Content-Type", "application/json")
        .put()
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();
    var responseJson = asJson(responseBody);

    var expectedResponse = loadResource(
        "preferences/update-kerberos-config-file-path-response.json");
    var expectedResponseJson = asJson(expectedResponse.formatted(configFilePath));
    assertEquals(expectedResponseJson, responseJson);

    assertEquals(
        configFilePath,
        System.getProperty(KerberosCredentials.KERBEROS_CONFIG_FILE_PROPERTY_NAME)
    );
  }

  /**
   * Get the certificates trusted by the provided web client.
   *
   * @param webClient The web client.
   * @return The list of certs trusted by the web client; empty list, if the web client does not
   * trust any custom cert.
   */
  List<String> getTrustedCertsOfWebClient(WebClient webClient) {
    try {
      var tokenField = webClient.getClass().getDeclaredField("options");
      tokenField.setAccessible(true);
      var options = (WebClientOptions) tokenField.get(webClient);
      var pemTrustOptions = (PemTrustOptions) options.getTrustOptions();

      if (pemTrustOptions == null) {
        return Collections.emptyList();
      }

      return pemTrustOptions.getCertPaths();
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the full path of the cert with the given name in the test/resources folder.
   *
   * @param certName Name of the cert
   * @return Full path to cert file.
   */
  String getCertPath(String certName) {
    return Thread
        .currentThread()
        .getContextClassLoader()
        .getResource(certName)
        .getFile();
  }
}
