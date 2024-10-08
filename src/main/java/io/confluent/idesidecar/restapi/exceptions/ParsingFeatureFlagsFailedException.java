package io.confluent.idesidecar.restapi.exceptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.ALWAYS)
public record ParsingFeatureFlagsFailedException(
    String code,
    String message
){
}