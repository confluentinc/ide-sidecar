package io.confluent.idesidecar.restapi.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.idesidecar.scaffolding.exceptions.InvalidTemplateOptionsProvided;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateNotFoundException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryIOException;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
class ExceptionMappersTest {

  @Inject
  ExceptionMappers exceptionMappers;

  @InjectMock
  UuidFactory uuidFactory;

  @BeforeEach
  void setUp() {
    Mockito.when(uuidFactory.getRandomUuid()).thenReturn("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4");
  }

  @Test
  void mapTemplateNotFoundException() {
    var exception = new TemplateNotFoundException("Template not found");
    checkFailureResponse(
        exceptionMappers.mapTemplateNotFoundException(exception),
        Status.NOT_FOUND,
        "Template not found",
        "resource_missing",
        List.of(
            new Failure.Error("resource_missing", "Template not found", "Template not found", null))
    );
  }

  @Test
  void mapGenericTemplateRegistryException() {
    var exception = new TemplateRegistryException(
        "Generic template registry exception",
        "generic_template_registry_exception"
    );
    checkFailureResponse(
        exceptionMappers.mapGenericTemplateRegistryException(exception),
        Status.INTERNAL_SERVER_ERROR,
        Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        "generic_template_registry_exception",
        List.of(new Failure.Error("generic_template_registry_exception",
            Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "Generic template registry exception", null))
    );
  }

  @Test
  void mapInvalidTemplateOptionsException() {
    var exception = new InvalidTemplateOptionsProvided(
        List.of(
            new TemplateManifest.Error(
                "missing_option", "Missing option", "Missing 'foo' option", "foo")
        )
    );

    checkFailureResponse(
        exceptionMappers.mapInvalidTemplateOptionsException(exception),
        Status.BAD_REQUEST,
        "Invalid template options provided",
        "invalid_template_options",
        List.of(
            new Failure.Error("missing_option", "Missing option", "Missing 'foo' option", "foo"))
    );
  }

  @Test
  void mapTemplateRegistryIOException() {
    var exception = new TemplateRegistryIOException(
        "Unexpected IO exception",
        "unexpected_io_exception"
    );
    checkFailureResponse(
        exceptionMappers.mapTemplateRegistryIOException(exception),
        Status.INTERNAL_SERVER_ERROR,
        Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        "unexpected_io_exception",
        List.of(new Failure.Error("unexpected_io_exception",
            Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
            "Unexpected IO exception", null))
    );
  }

  private void checkFailureResponse(
      Response response,
      Status status,
      String title,
      String code,
      List<Failure.Error> errors
  ) {
    assertEquals(status.getStatusCode(), response.getStatus());

    // All responses should be JSON
    assertEquals(MediaType.APPLICATION_JSON, response.getHeaderString(HttpHeaders.CONTENT_TYPE));

    // Check the response body
    Failure failure = response.readEntity(Failure.class);
    assertEquals(String.valueOf(status.getStatusCode()), failure.status());
    assertEquals(title, failure.title());
    assertEquals(code, failure.code());
    assertEquals(errors, failure.errors());
    assertEquals("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4", failure.id());
  }

}