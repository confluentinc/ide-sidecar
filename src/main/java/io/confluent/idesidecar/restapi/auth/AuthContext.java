package io.confluent.idesidecar.restapi.auth;

import io.vertx.core.Future;
import java.time.Instant;
import java.util.Optional;

/**
 * Interface for an authentication context that holds temporary tokens for interacting with an
 * external API, like Confluent Cloud. It exposes methods for refreshing tokens, verifying if tokens
 * are still valid, and investigating their expiration time.
 */
public interface AuthContext {

  /**
   * This method can be used to investigate the authentication status of the AuthContext. It returns
   * the current status and does not perform any kind of caching. The implementation of this method
   * might call external systems - don't call it too frequently.
   *
   * @return if the authentication context is authenticated
   */
  Future<Boolean> checkAuthenticationStatus();

  /**
   * This method returns the time at which the authentication context expires if there is any. The
   * expiration time can be in the future, in which case the context should be authenticated at the
   * time of calling this method, or in the past, in which case the context should no longer be
   * authenticated at the time of calling this method. If the authentication context holds multiple
   * tokens that have an expiration time, this method returns the earliest expiration time of any of
   * the tokens.
   *
   * @return an Optional describing the time at which the authentation context expires
   */
  Optional<Instant> expiresAt();

  /**
   * This method refreshes the tokens of the authentication context. NOTE: Unfortunately, we have to
   * accept an organizationId here since it's required by the CCloudOAuthContext implementation.
   * This is a design flaw that should be fixed in future iterations, or when we have more
   * implementations of this interface. But, we'll just have to live with it for now.
   *
   * @param organizationId the organization id to refresh the tokens for
   * @return the updated authentication context
   */
  Future<AuthContext> refresh(String organizationId);

  /**
   * Discard any existing tokens and reset the authentication context to the initial state.
   */
  void reset();
}
