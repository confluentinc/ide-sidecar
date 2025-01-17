package io.confluent.idesidecar.restapi.auth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.auth.AuthErrors.AuthError;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Instant;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class AuthErrorsTest {

  @Test
  void hasErrorsShouldReturnFalseIfAllErrorsAreNull() {
    var authErrorsWithoutErrors = new AuthErrors(null, null, null);
    assertFalse(authErrorsWithoutErrors.hasErrors());
  }

  @Test
  void hasNonTransientErrorsShouldReturnFalseIfAllErrorsAreNull() {
    var authErrorsWithoutErrors = new AuthErrors(null, null, null);
    assertFalse(authErrorsWithoutErrors.hasNonTransientErrors());
  }

  @Test
  void hasErrorsShouldReturnTrueIfAuthStatusCheckIsNotNull() {
    var authErrorsWithAuthStatusCheck = new AuthErrors(
        new AuthError("error", true),
        null,
        null);
    assertTrue(authErrorsWithAuthStatusCheck.hasErrors());
  }

  @Test
  void hasErrorsShouldReturnTrueIfSignInIsNotNull() {
    var authErrorsWithSignIn = new AuthErrors(
        null,
        new AuthError("error", false),
        null);
    assertTrue(authErrorsWithSignIn.hasErrors());
  }

  @Test
  void hasErrorsShouldReturnTrueIfTokenRefreshIsNotNull() {
    var authErrorsWithTokenRefresh = new AuthErrors(
        null,
        null,
        new AuthError("error", false));
    assertTrue(authErrorsWithTokenRefresh.hasErrors());
  }
}
