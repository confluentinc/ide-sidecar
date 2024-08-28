package io.confluent.idesidecar.restapi.exceptions;


import io.smallrye.graphql.api.ErrorCode;
import java.util.ArrayList;
import java.util.List;


/**
 * Exception thrown when a resource cannot be fetched when serving a GraphQL query.
 */
@ErrorCode("some-business-error-code")
public class ResourceFetchingException extends RuntimeException {

  public ResourceFetchingException(Failure failure, String failedAction) {
    super(extractMessage(failure, failedAction));
  }

  public ResourceFetchingException(
      ErrorResponse error, String failedAction) {
    super(extractMessage(error, failedAction));
  }

  public ResourceFetchingException(String message) {
    super(message);
  }

  public ResourceFetchingException(String message, Throwable cause) {
    super(message, cause);
  }

  private static String extractMessage(Failure failure, String action) {
    List<String> messages = new ArrayList<>();
    if (failure != null) {
      if (isNotBlank(failure.title())) {
        messages.add(failure.title());
      }
      if (failure.errors() != null) {
        failure.errors().forEach(error -> {
          List<String> parts = new ArrayList<>();
          if (isNotBlank(error.status())) {
            parts.add("(HTTP " + error.status() + ")");
          }
          if (isNotBlank(error.detail())) {
            parts.add(error.detail());
          }
          var message = String.join(" ", parts);
          if (isNotBlank(message)) {
            messages.add(message);
          }
        });
      }
    }
    if (messages.isEmpty()) {
      messages.add("Unknown error");
    }
    var count = messages.size();
    var message = String.join(", ", messages);
    if (isNotBlank(action)) {
      message = "%s failed with %d error(s): %s".formatted(action, count, message);
    }
    return message;
  }

  private static String extractMessage(ErrorResponse error, String action) {
    return "%s failed with %d error: %s".formatted(action, error.errorCode(), error.message());
  }

  private static boolean isNotBlank(String str) {
    return str != null && !str.isBlank();
  }
}