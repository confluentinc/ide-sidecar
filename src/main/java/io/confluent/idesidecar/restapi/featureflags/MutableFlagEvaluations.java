package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import java.util.Optional;

@RegisterForReflection
class MutableFlagEvaluations extends FlagEvaluations
    implements FlagMutations<MutableFlagEvaluations> {

  final String name;

  MutableFlagEvaluations(@NotNull String name) {
    super();
    this.name = name;
  }

  @Override
  public Optional<JsonNode> evaluateAsJson(String id) {
    return Optional.ofNullable(byId.get(id)).map(FlagEvaluation::value);
  }

  protected MutableFlagEvaluations add(@NotNull FlagEvaluation evaluation) {
    byId.put(
        evaluation.id(),
        evaluation
    );
    return this;
  }

  @Override
  public MutableFlagEvaluations add(@NotNull FlagId id, @NotNull Object value) {
    var jsonNode = OBJECT_MAPPER.valueToTree(value);
    return add(id, jsonNode);
  }

  @Override
  public MutableFlagEvaluations add(@NotNull FlagId id, @NotNull JsonNode value) {
    return add(new FlagEvaluation(id.toString(), name, value));
  }

  @Override
  public MutableFlagEvaluations addAll(Iterable<FlagEvaluation> evaluations) {
    for (FlagEvaluation eval : evaluations) {
      add(eval);
    }
    return self();
  }

  @Override
  public MutableFlagEvaluations remove(@NotNull FlagId id) {
    byId.remove(id.toString());
    return this;
  }

  @Override
  public MutableFlagEvaluations clear() {
    byId.clear();
    return this;
  }

  protected MutableFlagEvaluations self() {
    return this;
  }

  @Override
  public String toString() {
    return byId.values().toString();
  }
}
