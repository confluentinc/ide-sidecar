package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExceptionMappersTest {

  private static final Failure RATE_LIMITED = new Failure(
      Status.TOO_MANY_REQUESTS, "proxy_error", "Rate limited", "1", List.of()
  );

  @Test
  void processorFailureShouldCarryItsResponseHeaders() {
    var exception = new ProcessorFailedException(RATE_LIMITED, Map.of("Retry-After", "7"));

    var response = new ExceptionMappers().mapProcessorFailedException(exception);

    assertEquals(429, response.getStatus());
    assertEquals("7", response.getHeaderString("Retry-After"));
  }

  @Test
  void processorFailureWithoutHeadersShouldAddNone() {
    var response = new ExceptionMappers()
        .mapProcessorFailedException(new ProcessorFailedException(RATE_LIMITED));

    assertNull(response.getHeaderString("Retry-After"));
  }
}
