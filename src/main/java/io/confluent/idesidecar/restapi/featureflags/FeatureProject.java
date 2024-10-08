package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.sdk.LDContext;
import io.smallrye.common.constraint.NotNull;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * LaunchDarkly defines a project to include a set of feature flags that are accessed through a
 * specific URL with a project-specific client-side ID. This class encapsulates the logic of
 * fetching the project's flags and obtaining the latest evaluations for the specified context.
 */
public class FeatureProject {

  public interface Provider {

    /**
     * Evaluate the feature flags given the supplied context, returning the evaluations using the
     * supplied callback.
     *
     * @param context  the LaunchDarkly context for which the flags should be evaluated
     * @param callback the function to be called with the resulting set of {@link FlagEvaluation}s
     */
    void evaluateFlags(
        @NotNull LDContext context,
        @NotNull Consumer<Collection<FlagEvaluation>> callback
    );
  }

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  final String name;
  final String clientId;
  final String fetchUri;
  final AtomicReference<FlagEvaluations> evaluations = new AtomicReference<>(FlagEvaluations.EMPTY);
  final Provider provider;

  public FeatureProject(
      @NotNull String name,
      @NotNull String clientId,
      @NotNull String fetchUri
  ) {
    this.name = name;
    this.clientId = clientId;
    this.fetchUri = fetchUri;
    this.provider = new HttpFlagEvaluationProvider(name, clientId, this.fetchUri, OBJECT_MAPPER);
  }

  public FlagEvaluations evaluations() {
    return evaluations.get();
  }

  void refresh(LDContext context, CountDownLatch latch) {
    provider.evaluateFlags(
        context,
        latestEvaluations -> {
          // Only set the evaluations if the latest are non-null
          if (latestEvaluations != null) {
            evaluations.set(new FlagEvaluations(latestEvaluations));
          }
          // But always call the latch
          if (latch != null) {
            latch.countDown();
          }
        }
    );
  }

  @Override
  public String toString() {
    return name;
  }
}
