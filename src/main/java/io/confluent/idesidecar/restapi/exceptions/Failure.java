package io.confluent.idesidecar.restapi.exceptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import jakarta.ws.rs.core.Response.Status;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Provides information about problems encountered while performing an operation.
 *
 * @param status The HTTP status code applicable to this problem, expressed as a string value.
 * @param code   An application-specific error code, expressed as a string value.
 * @param title  A short, human-readable summary of the problem. It **SHOULD NOT** change from
 *               occurrence to occurrence of the problem, except for purposes of localization.
 * @param id     A unique identifier for this particular occurrence of the problem.
 * @param errors List of errors which caused this operation to fail
 */
@JsonPropertyOrder({
    "status",
    "code",
    "title",
    "id",
    "errors"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description =
    "Provides overall information about problems encountered while performing an operation.")
public record Failure(
    @JsonProperty
    String status,

    @JsonProperty
    String code,

    @JsonProperty
    String title,

    @JsonProperty
    String id,

    @JsonProperty
    List<Error> errors
) implements Serializable {

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  public Failure(Exception exception, Status status, String code, String title, String id,
      String source) {
    this(
        String.valueOf(status.getStatusCode()),
        code,
        title,
        id,
        List.of(new Error(code, title, exception.getMessage(), source))
    );
  }

  public Failure(Status status, String code, String title, String id, List<Error> errors) {
    this(
        String.valueOf(status.getStatusCode()),
        code,
        title,
        id,
        errors
    );
  }

  public Failure(List<Error> errors) {
    this((String) null, null, null, null, errors);
  }

  /**
   * Describes a particular error encountered while performing an operation.
   *
   * @param id     A unique identifier for this particular occurrence of the problem.
   * @param status The HTTP status code applicable to this problem, expressed as a string value.
   * @param code   An application-specific error code, expressed as a string value.
   * @param title  A short, human-readable summary of the problem. It **SHOULD NOT** change from
   *               occurrence to occurrence of the problem, except for purposes of localization.
   * @param detail A human-readable explanation specific to this occurrence of the problem.
   * @param source If this error was caused by a particular part of the API request, this is either
   *               a string or an object points to the query string parameter or request body
   *               property that caused it.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({
      "code",
      "status",
      "title",
      "id",
      "detail",
      "source"
  })
  @Schema(
      // Named this way so that JS/TS OpenAPI generated client code doesn't conflict with
      // the builtin `Error` class.
      name = "SidecarError",
      description = "Describes a particular error encountered while performing an operation."
  )
  public record Error(
      String id,
      String status,
      String code,
      String title,
      String detail,
      JsonNode source
  ) implements Serializable {

    public static Error create() {
      return new Error(null, null, null, null, null, null);
    }

    public Error(String code, String title, String detail, String source) {
      this(
          null,
          null,
          code,
          title,
          detail,
          source != null ? OBJECT_MAPPER.convertValue(source, JsonNode.class) : null
      );
    }

    public Error(String id, String status, String title, String detail, Map<String, ?> source) {
      this(
          id,
          status,
          null,
          title,
          detail,
          source != null ? OBJECT_MAPPER.convertValue(source, JsonNode.class) : null
      );
    }

    public Error withId(String id) {
      return new Error(id, status, code, title, detail, source);
    }

    public Error withStatus(String status) {
      return new Error(id, status, code, title, detail, source);
    }

    public Error withCode(String code) {
      return new Error(id, status, code, title, detail, source);
    }

    public Error withTitle(String title) {
      return new Error(id, status, code, title, detail, source);
    }

    public Error withDetail(String detail, Object... args) {
      return new Error(id, status, code, title, detail.formatted(args), source);
    }

    public Error withSource(String source, Object... args) {
      return new Error(
          id,
          status,
          code,
          title,
          detail,
          source != null ? OBJECT_MAPPER.convertValue(source.formatted(args), JsonNode.class) : null
      );
    }

    public Error withSource(Map<String, ?> source) {
      return new Error(
          id,
          status,
          code,
          title,
          detail,
          source != null ? OBJECT_MAPPER.convertValue(source, JsonNode.class) : null
      );
    }

    @SuppressWarnings({
        "CyclomaticComplexity",
        "NPathComplexity"
    })
    @Override
    public String toString() {
      // Custom implementation to avoid printing fields when they are null,
      // as most will be null much of the time
      var builder = new StringBuilder("Error{");
      boolean addDelim = false;
      if (id != null) {
        builder.append("id=").append(id);
        addDelim = true;
      }
      if (status != null) {
        if (addDelim) {
          builder.append(", ");
        }
        builder.append("status=").append(status);
        addDelim = true;
      }
      if (code != null) {
        if (addDelim) {
          builder.append(", ");
        }
        builder.append("code=").append(code);
        addDelim = true;
      }
      if (title != null) {
        if (addDelim) {
          builder.append(", ");
        }
        builder.append("title=").append(title);
        addDelim = true;
      }
      if (detail != null) {
        if (addDelim) {
          builder.append(", ");
        }
        builder.append("detail=").append(detail);
        addDelim = true;
      }
      if (source != null) {
        if (addDelim) {
          builder.append(", ");
        }
        builder.append("source=").append(source);
      }
      builder.append("}");
      return builder.toString();
    }
  }

  public String asJsonString() {
    return OBJECT_MAPPER.valueToTree(this).toString();
  }
}
