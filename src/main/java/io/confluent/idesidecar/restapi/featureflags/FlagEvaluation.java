package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

@RegisterForReflection
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record FlagEvaluation(
    /*
     * The identifier of the feature flag.
     */
    String id,

    /*
     * The name of the LaunchDarkly project where the feature flag is defined.
     */
    String project,
    @Nullable JsonNode value
) implements Comparable<FlagEvaluation> {

  @Override
  public int compareTo(@NotNull FlagEvaluation o) {
    if (o == this) {
      return 0;
    }
    return this.id.compareTo(o.id);
  }

  public FlagEvaluation withProject(String project) {
    return new FlagEvaluation(id, project, value);
  }

  public FlagEvaluation withId(String id) {
    return new FlagEvaluation(id, project, value);
  }
}