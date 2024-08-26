package io.confluent.idesidecar.restapi.auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;

public record Token(@JsonIgnore String token, Instant expiresAt) {

}
