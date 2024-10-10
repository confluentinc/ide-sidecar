package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.json.JsonSerialization;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * A {@link FeatureProject.Provider} that makes a remote HTTP call to LaunchDarkly APIs to
 * evaluate and return the feature flags for the given context.
 */
@RegisterForReflection
class HttpFlagEvaluationProvider implements FeatureProject.Provider {

  final String projectName;
  final ObjectMapper objectMapper;
  final String clientId;
  final String fetchUri;

  HttpFlagEvaluationProvider(
      @NotNull String projectName,
      @NotNull String clientId,
      @NotNull String fetchUri,
      @NotNull ObjectMapper objectMapper
  ) {
    this.projectName = projectName;
    this.clientId = clientId;
    this.fetchUri = fetchUri;
    this.objectMapper = objectMapper;
  }

  @Override
  public void evaluateFlags(
      @NotNull LDContext context,
      @NotNull WebClientFactory webClientFactory,
      @NotNull Consumer<Collection<FlagEvaluation>> callback
  ) {
    var client = webClientFactory.getWebClient();
    String encodedContext;
    try {
      encodedContext = getBase64EncodedContext(context);
    } catch (JsonProcessingException e) {
      Log.errorf("Failed to evaluate feature flags for project '%s', "
                 + "due to error encoding context in base64", projectName, e);
      callback.accept(null);
      return;
    }

    // Construct the URL
    var url = uri(encodedContext);
    Log.debugf("Evaluating feature flags for project '%s' with GET %s", projectName, url);

    // Submit a request to evaluate the flags for the given context
    client.getAbs(url)
          .send()
          .map(result ->
              FlagEvaluations.parseResponse(
                  result.bodyAsString(),
                  objectMapper,
                  eval -> eval.withProject(projectName)
              )
          )
          .onSuccess(flags -> {
            Log.debugf(
                "Updated %d feature flags for project '%s'",
                flags.size(),
                projectName
            );
            callback.accept(flags);
          })
          .onFailure(failure -> {
            // This is debug because a user running disconnected from network should not see errors
            Log.debugf(
                "Error evaluating feature flags for project '%s': %s",
                projectName,
                failure.getMessage(),
                failure
            );
            // Use the callback, but pass null (not empty) signifying we have no new evaluations
            callback.accept(null);
          });
  }

  String uri(String evalContext) {
    return String.format(fetchUri, clientId, evalContext);
  }

  static String getBase64EncodedContext(
      @NotNull LDContext evalContext
  ) throws JsonProcessingException {
    // Serialize the context as JSON
    var jsonStr = JsonSerialization.serialize(evalContext);
    // then encode as base64
    byte[] encodedBytes = Base64.getEncoder()
                                .encode(jsonStr.getBytes(StandardCharsets.UTF_8));
    var base64Str = new String(encodedBytes, StandardCharsets.UTF_8);
    // and URL-encode it
    return URLEncoder.encode(base64Str, StandardCharsets.UTF_8);
  }
}
