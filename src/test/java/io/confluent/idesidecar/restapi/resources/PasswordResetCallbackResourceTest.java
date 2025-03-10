package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.not;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
public class PasswordResetCallbackResourceTest {

  @Test
  void shouldCompleteResetPassworFlowIfAllParamsAreProvided() {
    given()
        .queryParam("email", "ide-sidecar@confluent.io")
        .queryParam("success", "true")
        .queryParam("message", "Password reset successfully")
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .body(containsString("Reset Password Complete"))
        .body(containsString(OAuthCallbackResource.CCLOUD_OAUTH_VSCODE_EXTENSION_URI))
        .body(containsString("?success=false&reset_password=true"));
  }

  @Test
  void shouldNotCompleteResetPassworFlowIfSuccessParamIsSetToFalse() {
    given()
        .queryParam("email", "ide-sidecar@confluent.io")
        .queryParam("success", "false")
        .queryParam("message", "Password reset successfully")
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .body(not(containsString("Reset Password Complete")))
        .body(containsString(OAuthCallbackResource.CCLOUD_OAUTH_VSCODE_EXTENSION_URI))
        .body(not(containsString("?success=false&reset_password=true")));
  }

  @Test
  void shouldNotCompleteResetPassworFlowIfSuccessParamIsMissing() {
    given()
        .queryParam("email", "ide-sidecar@confluent.io")
        .queryParam("message", "Password reset successfully")
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .body(not(containsString("Reset Password Complete")))
        .body(containsString(OAuthCallbackResource.CCLOUD_OAUTH_VSCODE_EXTENSION_URI))
        .body(not(containsString("?success=false&reset_password=true")));
  }

  @Test
  void shouldNotCompleteResetPassworFlowIfEmailParamIsMissing() {
    given()
        .queryParam("success", "true")
        .queryParam("message", "Password reset successfully")
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .body(not(containsString("Reset Password Complete")))
        .body(containsString(OAuthCallbackResource.CCLOUD_OAUTH_VSCODE_EXTENSION_URI))
        .body(not(containsString("?success=false&reset_password=true")));
  }

  @Test
  void shouldNotCompleteResetPassworFlowIfMessageParamIsMissing() {
    given()
        .queryParam("email", "ide-sidecar@confluent.io")
        .queryParam("success", "true")
        .when()
        .get("/")
        .then()
        .statusCode(200)
        .body(not(containsString("Reset Password Complete")))
        .body(containsString(OAuthCallbackResource.CCLOUD_OAUTH_VSCODE_EXTENSION_URI))
        .body(not(containsString("?success=false&reset_password=true")));
  }
}
