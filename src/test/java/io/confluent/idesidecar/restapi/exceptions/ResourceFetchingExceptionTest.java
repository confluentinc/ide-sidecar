package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import jakarta.ws.rs.core.Response;
import java.util.List;
import org.junit.jupiter.api.Test;

class ResourceFetchingExceptionTest {

  private static final String ACTION = "GET http://localhost/v1/something";

  @Test
  void shouldCreateFromActionAndFailureWithNoErrors() {
    var exception = new ResourceFetchingException(
        new Failure(
            Response.Status.NOT_FOUND,
            "code",
            "title",
            "id",
            List.of()
        ),
        ACTION
    );
    assertEquals(
        ACTION + " failed with 1 error(s): title",
        exception.getMessage()
    );
  }

  @Test
  void shouldCreateFromActionAndFailureWithOneError() {
    var exception = new ResourceFetchingException(
        new Failure(
            List.of(
                Failure.Error.create().withId("ID1").withStatus("404").withDetail("Not found")
            )
        ),
        ACTION
    );
    assertEquals(
        ACTION + " failed with 1 error(s): (HTTP 404) Not found",
        exception.getMessage()
    );
  }

  @Test
  void shouldCreateFromActionAndFailureWithTitleAndMultipleErrors() {
    var exception = new ResourceFetchingException(
        new Failure(
            Response.Status.NOT_FOUND,
            "code",
            "title",
            "id",
            List.of(
                Failure.Error.create().withId("ID1").withStatus("404").withDetail("Not found"),
                Failure.Error.create().withId("ID2").withStatus("403").withDetail("Forbidden"),
                Failure.Error.create().withId("ID3").withStatus("402"),
                Failure.Error.create().withId("ID2").withStatus("401").withDetail("Unauthorized")
            )
        ),
        ACTION
    );
    assertEquals(
        ACTION + " failed with 5 error(s): title, (HTTP 404) Not found, (HTTP 403) Forbidden, (HTTP 402), (HTTP 401) Unauthorized",
        exception.getMessage()
    );
  }

  @Test
  void shouldCreateFromActionAndFailureWithMultipleErrors() {
    var exception = new ResourceFetchingException(
        new Failure(
            List.of(
                Failure.Error.create().withId("ID1").withStatus("404").withDetail("Not found"),
                Failure.Error.create().withId("ID2").withStatus("403").withDetail("Forbidden"),
                Failure.Error.create().withId("ID3").withStatus("402"),
                Failure.Error.create().withId("ID2").withStatus("401").withDetail("Unauthorized")
            )
        ),
        ACTION
    );
    assertEquals(
        ACTION + " failed with 4 error(s): (HTTP 404) Not found, (HTTP 403) Forbidden, (HTTP 402), (HTTP 401) Unauthorized",
        exception.getMessage()
    );
  }

  @Test
  void shouldCreateFromFailureWithNullAction() {
    var exception = new ResourceFetchingException(
        new Failure(
            List.of(
                Failure.Error.create().withId("ID1").withStatus("404").withDetail("Not found"),
                Failure.Error.create().withId("ID2").withStatus("403").withDetail("Forbidden"),
                Failure.Error.create().withId("ID3").withStatus("402"),
                Failure.Error.create().withId("ID2").withStatus("401").withDetail("Unauthorized")
            )
        ),
        null
    );
    assertEquals(
        "(HTTP 404) Not found, (HTTP 403) Forbidden, (HTTP 402), (HTTP 401) Unauthorized",
        exception.getMessage()
    );
  }

  @Test
  void shouldCreateFromFailureWithBlankAction() {
    var exception = new ResourceFetchingException(
        new Failure(
            List.of(
                Failure.Error.create().withId("ID1").withStatus("404").withDetail("Not found"),
                Failure.Error.create().withId("ID2").withStatus("403").withDetail("Forbidden"),
                Failure.Error.create().withId("ID3").withStatus("402"),
                Failure.Error.create().withId("ID2").withStatus("401").withDetail("Unauthorized")
            )
        ),
        "  "
    );
    assertEquals(
        "(HTTP 404) Not found, (HTTP 403) Forbidden, (HTTP 402), (HTTP 401) Unauthorized",
        exception.getMessage()
    );
  }
}