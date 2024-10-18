package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.idesidecar.restapi.auth.AuthErrors;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.Authentication.Status;
import java.time.Instant;

/**
 * Represents the status of a connection.
 */
public record ConnectionStatus(@JsonProperty(required = true) Authentication authentication) {

  /**
   * Initial status of any connection. A new connection does not hold any token, does
   * not know the end of the lifetime of its tokens, and has not faced any errors.
   */
  public static final ConnectionStatus INITIAL_STATUS =
      new ConnectionStatus(new Authentication(Status.NO_TOKEN, null, null, null));

  @JsonInclude(Include.NON_NULL)
  public record Authentication(
      @JsonProperty(required = true) Status status,
      /**
       * If the connection's auth context holds a valid token, this attribute holds the time at
       * which the user must re-authenticate because, for instance, the refresh token reached the
       * end of its absolute lifetime.
       */
      @JsonProperty(value = "requires_authentication_at") Instant requiresAuthenticationAt,

      /**
       * Information about the user of the authenticated principal if provided by the connection.
       */
      UserInfo user,
      /**
       * Auth-related errors of this connection.
       */
      AuthErrors errors
  ) {

    /**
     * A connection can be in one of four states: Initially, a connection does not hold any tokens
     * and is in the state NO_TOKEN. If the connection holds tokens, these tokens can be valid or
     * invalid. Depending on the health of the tokens, the connection is in the state VALID_TOKEN or
     * INVALID_TOKEN. The connection will regularly refresh tokens before they expire to keep them
     * valid. If the connection experienced a non-transient error, it will enter the state
     * FAILED from which it can't recover automatically. Consumers of the API are advised to trigger
     * a new authentication flow to recover from the FAILED state.
     */
    public enum Status {
      NO_TOKEN,
      VALID_TOKEN,
      INVALID_TOKEN,
      FAILED
    }
  }

  /**
   * The information known about the user of this connection. Any of these values can be null
   *
   * @param id        the system-specific identifier for the user, if different than the username
   * @param username  the username or email address
   * @param firstName the user's first name, if known
   * @param lastName  the user's last name, if known
   */
  @JsonInclude(Include.NON_NULL)
  public record UserInfo(

      String id,
      String username,
      @JsonProperty(value = "first_name") String firstName,
      @JsonProperty(value = "last_name") String lastName,
      @JsonProperty(value = "social_connection") String socialConnection,
      @JsonProperty(value = "auth_type") String authType
  ) {

  }
}