package io.confluent.idesidecar.restapi.exceptions;

import io.confluent.idesidecar.restapi.kafkarest.UnknownAlterConfigOperation;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
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
        .status(extractStatus(failure))
        .entity(failure)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  private int extractStatus(Failure failure) {
    // Either get status if set, or look through errors
    if (failure.status() != null) {
      return Integer.parseInt(failure.status());
    } else if (failure.errors() != null && !failure.errors().isEmpty()) {
      return Integer.parseInt(failure.errors().getFirst().status());
    } else {
      return Status.INTERNAL_SERVER_ERROR.getStatusCode();
    }
  }

  @ServerExceptionMapper
  public Response mapInvalidPreferencesException(InvalidPreferencesException exception) {
    var failure = new Failure(
        Status.BAD_REQUEST,
        "invalid_preferences",
        "Provided preferences are not valid",
        uuidFactory.getRandomUuid(),
        exception.errors()
    );

    return Response
        .status(Status.BAD_REQUEST)
        .entity(failure)
        // Explicitly set the content type to JSON here
        // since the resource method may have set it to something else
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapFlagNotFoundException(
      FlagNotFoundException exception
  ) {

    Failure failure = new Failure(
        exception,
        Status.NOT_FOUND,
        "resource_missing", exception.getMessage(), uuidFactory.getRandomUuid(),
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


  @ServerExceptionMapper
  public Response mapUnknownTopicException(UnknownTopicOrPartitionException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.NOT_FOUND.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.NOT_FOUND)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapTopicAlreadyExistsException(TopicExistsException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.CONFLICT.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.CONFLICT)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapClusterNotFoundException(ClusterNotFoundException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.NOT_FOUND.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.NOT_FOUND)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapKafkaTimeoutException(TimeoutException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.REQUEST_TIMEOUT.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.REQUEST_TIMEOUT)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapUnsupportedException(UnsupportedOperationException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.NOT_IMPLEMENTED.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.NOT_IMPLEMENTED)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapApiException(ApiException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.INTERNAL_SERVER_ERROR)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapRestClientException(RestClientException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(exception.getErrorCode())
        .message(exception.getMessage()).build();
    return Response
        .status(exception.getStatus())
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapBadRequestException(BadRequestException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.BAD_REQUEST.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.BAD_REQUEST)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapUnknownAlterConfigOperation(UnknownAlterConfigOperation exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.BAD_REQUEST.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.BAD_REQUEST)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapKafkaInvalidConfigurationException(InvalidConfigurationException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.BAD_REQUEST.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.BAD_REQUEST)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }

  @ServerExceptionMapper
  public Response mapKafkaInvalidRequestException(InvalidRequestException exception) {
    var error = io.confluent.idesidecar.restapi.kafkarest.model.Error
        .builder()
        .errorCode(Status.BAD_REQUEST.getStatusCode())
        .message(exception.getMessage()).build();
    return Response
        .status(Status.BAD_REQUEST)
        .entity(error)
        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
        .build();
  }
}
