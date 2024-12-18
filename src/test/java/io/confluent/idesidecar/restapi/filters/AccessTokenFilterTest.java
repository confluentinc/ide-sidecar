package io.confluent.idesidecar.restapi.filters;

import static org.hamcrest.CoreMatchers.is;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;


@QuarkusTest
public class AccessTokenFilterTest {

  // Since the filter is expressed in terms of only opting out of covering some routes,
  // we need to hand-track what routes are guarded by the filter.
  static final String[] KNOWN_GUARDED_ROUTES = new String[]{
      "/gateway/v1/connections",
      "/gateway/v1/health/ready",
      "/gateway/v1/templates",
      // anything under the kafka meta proxy route
      "/kafka/v3/",
      "/subjects/fake-subject/versions/fake-version/schema",
      "/schemas/id/fake-schema-id/subjects",
      // the filter should summarily reject before the graphql endpoint
      // has chance to complain about not being a POST
      "/gateway/v1/graphql",
      // the websocket route
      "/ws"
  };

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  /**
   * Test that the filter allows requests that have the proper access token.
   */
  @Test
  void testSuccessWithRightAccessToken() {

    String expectedTokenValue = "3456dfg3456";
    setAuthToken(expectedTokenValue);

    // Make a hit to a guarded route accepting bare GET requests with the right bearer token value.
    RestAssured.given()
        .when()
        .header("Authorization", "Bearer " + expectedTokenValue)
        .get("/gateway/v1/connections")
        .then()
        .assertThat()
        .statusCode(200);

  }

  private static ArgumentSets tokenAndKnownGuardedRoutes() {
    return ArgumentSets
        .argumentsForFirstParameter(
            null, // as if handshake route not hit yet. Filter should still reject.
            "sdfsdfsdfsdf" // as if handshake route had been hit and this was generated token.
        )
        .argumentsForNextParameter(KNOWN_GUARDED_ROUTES);
  }

  /**
   * The access token filter must cover requests to the following routes and will reject with 401 if
   * improper access token is provided. Additionally, the filter will inject the sidecar's PID into
   * the response headers (in case the extension needs to kill us)
   */
  @CartesianTest
  @CartesianTest.MethodFactory("tokenAndKnownGuardedRoutes")
  void testRoutesRejectIfNoAccessToken(String token, String route) {
    String myPid = String.valueOf(ProcessHandle.current().pid());
    setAuthToken(token);

    // Make a hit w/o any authorization header
    RestAssured.given()
        .when()
        .get(route)
        .then()
        .assertThat()
        .statusCode(401)
        .header("x-sidecar-pid", myPid)
        .header("Content-Type", is("application/json"))
        .body("title", is("Missing or invalid access token."))
        .body("errors[0].detail", is("Missing or invalid access token."));

    // Various wrong auth header values. Gets all branches covered.
    for (String badAuthHeaderValue : new String[]{
        "sdfsdfsdfsdf",
        "Foonly bar",
        "Bearer",
        "Bearer ",
        "Bearer wrong-token",
        "Bearer " + token + "extra"
    }) {
      RestAssured.given()
          .when()
          .header("Authorization", badAuthHeaderValue)
          .get(route)
          .then()
          .assertThat()
          .statusCode(401)
          .header("x-sidecar-pid", myPid);
    }
  }

  /**
   * Explicitly set the access token value in the SidecarAccessTokenBean, consulted by the filter.
   *
   * @param value The value to set the access token to.
   */
  void setAuthToken(String value) {
    try {
      Field tokenField = accessTokenBean.getClass().getDeclaredField("token");
      tokenField.setAccessible(true);
      tokenField.set(accessTokenBean, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
