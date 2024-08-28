package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.idesidecar.scaffolding.exceptions.InvalidTemplateOptionsProvided;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateNotFoundException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryIOException;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Maps exceptions to instances of {@link Response}.
 */
public class ExceptionMappers {

  @Inject
  UuidFactory uuidFactory;

  @ServerExceptionMapper
  public Response mapProcessorFailedException(ProcessorFailedException exception) {
    var failure = exception.getFailure();
    return Response
        .status(Integer.parseInt(failure.status()))
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapTemplateNotFoundException(
      TemplateNotFoundException exception) {

    Failure failure = new Failure(
        exception,
        Status.NOT_FOUND,
        "resource_missing", "Template not found", uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.NOT_FOUND)
        .entity(failure)
        // Explicitly set the content type to JSON here
        // since the resource method may have set it to something else
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }


  @ServerExceptionMapper
  public Response mapGenericTemplateRegistryException(TemplateRegistryException exception) {
    Failure failure = new Failure(
        exception,
        Status.INTERNAL_SERVER_ERROR,
        exception.getCode(), Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.INTERNAL_SERVER_ERROR)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapInvalidTemplateOptionsException(
      InvalidTemplateOptionsProvided exception) {
    var errors = exception.getErrors().stream()
        .map(error -> new Error(error.code(), error.title(), error.detail(), error.source()))
        .toList();
    Failure failure = new Failure(
        Status.BAD_REQUEST,
        exception.getCode(), exception.getMessage(), uuidFactory.getRandomUuid(),
        errors
    );
    return Response
        .status(Status.BAD_REQUEST)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapTemplateRegistryIOException(
      TemplateRegistryIOException exception) {
    Failure failure = new Failure(
        exception,
        Status.INTERNAL_SERVER_ERROR,
        exception.getCode(), Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.INTERNAL_SERVER_ERROR)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapConnectionNotFoundException(ConnectionNotFoundException exception) {
    Failure failure = new Failure(
        exception,
        Status.NOT_FOUND,
        "None", Status.NOT_FOUND.getReasonPhrase(),
        uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.NOT_FOUND)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapCCloudAuthenticationFailedException(
      CCloudAuthenticationFailedException exception) {
    Failure failure = new Failure(
        exception,
        Status.UNAUTHORIZED,
        "unauthorized", Status.UNAUTHORIZED.getReasonPhrase(),
        uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.UNAUTHORIZED)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }


  @ServerExceptionMapper
  public Response mapCreateConnectionException(CreateConnectionException exception) {
    Failure failure = new Failure(
        exception,
        Status.CONFLICT,
        "None", Status.CONFLICT.getReasonPhrase(),
        uuidFactory.getRandomUuid(),
        null);
    return Response
        .status(Status.CONFLICT)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapInvalidInputException(InvalidInputException exception) {
    Failure failure = new Failure(
        Status.BAD_REQUEST,
        "invalid_input",
        exception.getMessage(),
        uuidFactory.getRandomUuid(),
        exception.errors()
    );
    return Response
        .status(Status.BAD_REQUEST)
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }
}
