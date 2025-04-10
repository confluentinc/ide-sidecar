package io.confluent.idesidecar.restapi.auth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This record holds auth-related errors for a {@link AuthContext}.
 *
 * @param authStatusCheck Error that occurred when checking the auth status.
 * @param signIn          Error that occurred when performing the sign in.
 * @param tokenRefresh    Error that occurred when refreshing tokens.
 */
@JsonInclude(Include.NON_NULL)
public record AuthErrors(
    @JsonProperty(value = "auth_status_check") AuthError authStatusCheck,
    @JsonProperty(value = "sign_in") AuthError signIn,
    @JsonProperty(value = "token_refresh") AuthError tokenRefresh
) {

  public AuthErrors() {
    this(null, null, null);
  }

  public boolean hasErrors() {
    return authStatusCheck != null || signIn != null || tokenRefresh != null;
  }

  public boolean hasNonTransientErrors() {
    boolean nonTransientAuthStatusCheckError =
        authStatusCheck != null && Boolean.FALSE.equals(authStatusCheck.isTransient);
    boolean nonTransientSignInError =
        signIn != null && Boolean.FALSE.equals(signIn.isTransient);
    boolean nonTransientTokenRefreshError =
        tokenRefresh != null && Boolean.FALSE.equals(tokenRefresh.isTransient);

    return nonTransientAuthStatusCheckError
        || nonTransientSignInError
        || nonTransientTokenRefreshError;
  }

  public AuthErrors withAuthStatusCheck(String message) {
    return new AuthErrors(
        // Always consider errors that occurred when checking the auth status as transient
        new AuthError(message, true),
        signIn,
        tokenRefresh
    );
  }

  public AuthErrors withoutAuthStatusCheck() {
    return new AuthErrors(null, signIn, tokenRefresh);
  }

  public AuthErrors withSignIn(String message) {
    return new AuthErrors(
        authStatusCheck,
        // Always consider errors that occurred when signing in as non-transient
        new AuthError(message, false),
        tokenRefresh
    );
  }

  public AuthErrors withoutSignIn() {
    return new AuthErrors(authStatusCheck, null, tokenRefresh);
  }

  public AuthErrors withTokenRefresh(String message, Boolean isTransient) {
    return new AuthErrors(
        authStatusCheck,
        signIn,
        new AuthError(message, isTransient)
    );
  }

  public AuthErrors withoutTokenRefresh() {
    return new AuthErrors(authStatusCheck, signIn, null);
  }

  public record AuthError(
      String message,
      @JsonProperty(value = "is_transient") Boolean isTransient
  ) {

  }
}
