package io.confluent.idesidecar.restapi.exceptions;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A Confluent API error response from the Confluent Kafka REST API
 *
 * @param errorCode the error code
 * @param message   the error message
 */
public record ErrorResponse(
    @JsonProperty("error_code") int errorCode,
    String message
) {
}
