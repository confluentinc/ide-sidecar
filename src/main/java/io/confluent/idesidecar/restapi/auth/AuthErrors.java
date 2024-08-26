package io.confluent.idesidecar.restapi.auth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

/**
 * This record holds auth-related errors for a {@link AuthContext}.
 *
 * @param authStatusCheck Error that occurred when checking the auth status.
 * @param signIn Error that occurred when performing the sign in.
 * @param tokenRefresh Error that occurred when refreshing tokens.
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

  public AuthErrors withAuthStatusCheck(String message) {
    return new AuthErrors(new AuthError(Instant.now(), message), signIn, tokenRefresh);
  }

  public AuthErrors withoutAuthStatusCheck() {
    return new AuthErrors(null, signIn, tokenRefresh);
  }

  public AuthErrors withSignIn(String message) {
    return new AuthErrors(authStatusCheck, new AuthError(Instant.now(), message), tokenRefresh);
  }

  public AuthErrors withoutSignIn() {
    return new AuthErrors(authStatusCheck, null, tokenRefresh);
  }

  public AuthErrors withTokenRefresh(String message) {
    return new AuthErrors(authStatusCheck, signIn, new AuthError(Instant.now(), message));
  }

  public AuthErrors withoutTokenRefresh() {
    return new AuthErrors(authStatusCheck, signIn, null);
  }

  public record AuthError(
      @JsonProperty(value = "created_at") Instant createdAt,
      String message
  ) {
  }
}
