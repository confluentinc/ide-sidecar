package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import java.util.List;
import java.util.Optional;

/**
 * Interface for changing {@link FlagEvaluation flag evaluations}.
 *
 * @param <SelfT> the concrete type of this object
 * @see FeatureFlags#defaults()
 * @see FeatureFlags#overrides()
 */
@RegisterForReflection
public interface FlagMutations<SelfT extends FlagMutations<SelfT>> {

  /**
   * Get the JSON representation of the current value of the flag with the given ID from any of the
   * projects.
   *
   * @param id the ID of the flag to find
   * @return the current value of the flag, or empty if the flag does not exist
   */
  Optional<JsonNode> evaluateAsJson(String id);

  /**
   * Add the given {@link FlagEvaluation} objects.
   *
   * @param evaluations the evaluations to add
   * @return this object for method chaining purposes; never null
   */
  SelfT addAll(@NotNull Iterable<FlagEvaluation> evaluations);

  /**
   * Use the given boolean as the value the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  default SelfT add(@NotNull FlagId id, boolean value) {
    return add(id, BooleanNode.valueOf(value));
  }

  /**
   * Use the given number as the value the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  default SelfT add(@NotNull FlagId id, int value) {
    return add(id, IntNode.valueOf(value));
  }

  /**
   * Use the given number as the value the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  default SelfT add(@NotNull FlagId id, long value) {
    return add(id, LongNode.valueOf(value));
  }

  /**
   * Use the given string as the value the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  default SelfT add(@NotNull FlagId id, @NotNull String value) {
    return add(id, TextNode.valueOf(value));
  }

  /**
   * Use the given list of {@link Notice} as the value for the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  default SelfT add(@NotNull FlagId id, @NotNull List<Notice> value) {
    return add(id, (Object) value);
  }

  /**
   * Use the given object as the value the {@link FlagId}. The object will be converted into a
   * {@link JsonNode}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  SelfT add(@NotNull FlagId id, @NotNull Object value);

  /**
   * Use the given JSON node as the value the {@link FlagId}.
   *
   * @param id    the identifier of the flag
   * @param value the new value for the flag
   * @return this object for method chaining purposes; never null
   */
  SelfT add(@NotNull FlagId id, @NotNull JsonNode value);

  /**
   * Use null as the value the {@link FlagId}. This is not the same thing as
   * {@link #remove(FlagId) removing the flag}, as some flags may be defined to have a null value.
   *
   * @param id the identifier of the flag
   * @return this object for method chaining purposes; never null
   * @see #remove(FlagId)
   */
  default SelfT addNull(@NotNull FlagId id) {
    return add(id, NullNode.getInstance());
  }

  /**
   * Remove the flag with the given {@link FlagId}.
   *
   * @param id the identifier of the flag
   * @return this object for method chaining purposes; never null
   * @see #addNull(FlagId)
   */
  SelfT remove(@NotNull FlagId id);

  /**
   * Clear all flags defined in this collection.
   *
   * @return this object for method chaining purposes; never null
   */
  SelfT clear();
}
