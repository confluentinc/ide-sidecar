package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.sdk.LDContext;
import com.launchdarkly.sdk.json.JsonSerialization;
import io.confluent.idesidecar.restapi.exceptions.FeatureFlagFailureException;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

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
  final boolean testMode;

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
    this.testMode = fetchUri.startsWith("http://localhost");
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
            // Signal that we do have new evaluations
            callback.accept(flags);
          })
          .onFailure(failure -> {
            if (failure instanceof FeatureFlagFailureException) {
              var level = testMode ? Logger.Level.DEBUG : Logger.Level.ERROR;
              // This occurs when we're unable to parse the evaluation response or error response
              // from the provider. We DO want to log these, as the problem needs to be fixed.
              Log.logf(
                  level,
                  "Error evaluating feature flags for project '%s': %s",
                  projectName,
                  failure.getMessage(),
                  failure
              );
            } else {
              // This occurs when there are any other problems evaluating feature flags, including
              // network issues due to running without a connection to the internet or LD is down.
              // These are anticipated conditions that do not signal a problem with this code, so
              // we DO NOT want the user to see these.
              Log.debugf(
                  "Error evaluating feature flags for project '%s': %s",
                  projectName,
                  failure.getMessage(),
                  failure
              );
            }
            // Pass to the callback a null (not empty) list, signifying we have no new evaluations
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
