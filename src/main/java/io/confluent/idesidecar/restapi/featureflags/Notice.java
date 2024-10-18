package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.Nullable;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Notice(
    String message,
    @Nullable String suggestion
) {

}
