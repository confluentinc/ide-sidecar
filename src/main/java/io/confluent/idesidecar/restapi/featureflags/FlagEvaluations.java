package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.exceptions.FeatureFlagFailureException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * A container for a set of {@link FlagEvaluation} instances. It is immutable and can be used to
 * list all flags or find flags by ID.
 */
@RegisterForReflection
class FlagEvaluations implements Iterable<FlagEvaluation> {

  public static final FlagEvaluations EMPTY = new FlagEvaluations(List.of());

  static final String DEFAULT_FLAG_EVALUATIONS_RESOURCE = "feature-flag-defaults.json";

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static FlagEvaluations defaults() {
    return loadFromClasspath(
        DEFAULT_FLAG_EVALUATIONS_RESOURCE,
        "default",
        "default feature flags"
    );
  }

  public static FlagEvaluations loadFromClasspath(
      String resourcePath,
      String projectName,
      String description
  ) {
    // Get the content from the resource file
    try (var inputStream = Thread
        .currentThread()
        .getContextClassLoader()
        .getResourceAsStream(resourcePath)) {
      if (inputStream != null) {
        var content = inputStream.readAllBytes();
        var evaluations = parseResponse(
            new String(content, StandardCharsets.UTF_8),
            OBJECT_MAPPER,
            eval -> eval.withProject(projectName));
        return new FlagEvaluations(evaluations);
      }
    } catch (IOException e) {
      var msg = "Error parsing %s from resource file '%s'".formatted(description, resourcePath);
      Log.error(msg, e);
    }
    return FlagEvaluations.EMPTY;
  }

  static Collection<FlagEvaluation> parseResponse(
      @NotNull String json,
      @NotNull ObjectMapper objectMapper,
      @NotNull UnaryOperator<FlagEvaluation> modifier
  ) {
    try {
      var byName = objectMapper.readValue(json,
          new TypeReference<Map<String, FlagEvaluation>>() {
          }
      );
      // Modify the map to add the id and apply modifier
      return byName.entrySet()
          .stream()
          .map(entry -> entry.getValue().withId(entry.getKey()))
          .map(modifier)
          .sorted()
          .toList();
    } catch (JsonProcessingException e) {
      try {
        // Try to read the error returned from LD
        var error = objectMapper.readValue(json, ParsingError.class);
        if (error != null) {
          throw new FeatureFlagFailureException(error.message, error.code, e);
        } else {
          throw new FeatureFlagFailureException(e);
        }
      } catch (JsonProcessingException e2) {
        throw new FeatureFlagFailureException(e2);
      }
    }
  }

  @RegisterForReflection
  record ParsingError(
      String code,
      String message
  ) {

  }

  final Map<String, FlagEvaluation> byId = new ConcurrentHashMap<>();

  FlagEvaluations(@NotNull Iterable<FlagEvaluation> evaluations) {
    evaluations.forEach(eval -> {
      byId.put(eval.id(), eval);
    });
  }

  FlagEvaluations() {
  }

  public Map<String, FlagEvaluation> asMap() {
    return Map.copyOf(byId);
  }

  @Override
  public Iterator<FlagEvaluation> iterator() {
    return byId.values().iterator();
  }

  public Collection<FlagEvaluation> evaluations() {
    return byId.values();
  }

  public Optional<FlagEvaluation> evaluation(@NotNull String id) {
    return Optional.ofNullable(byId.get(id));
  }

  public int size() {
    return byId.size();
  }

  public boolean isEmpty() {
    return byId.isEmpty();
  }
}
