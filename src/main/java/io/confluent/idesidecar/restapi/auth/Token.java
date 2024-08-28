package io.confluent.idesidecar.restapi.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;

/**
 * Models a Confluent Cloud API token.
 *
 * @param token     The token
 * @param expiresAt The time at which the token expires
 */
public record Token(@JsonIgnore String token, Instant expiresAt) {

}
