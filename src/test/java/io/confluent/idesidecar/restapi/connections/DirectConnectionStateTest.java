package io.confluent.idesidecar.restapi.connections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.clients.RetryAfterException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.net.ConnectException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class DirectConnectionStateTest {

  private static final String URI = "http://localhost:8081";

  @Test
  void schemaRegistryErrorMessageShouldPassOnARequestToRetryLater() {
    var busy = new RetryAfterException(
        new RestClientException("Service Unavailable", 503, 50301),
        Duration.ofSeconds(120),
        "retries ran out"
    );

    var message = DirectConnectionState.schemaRegistryErrorMessage(busy, URI);

    assertEquals(
        "Schema Registry asked to retry after 2m, but retries ran out: Service Unavailable; "
            + "error code: 50301 (http://localhost:8081)",
        message
    );
  }

  @Test
  void schemaRegistryErrorMessageShouldSayUnreachableWhenTheConnectionFails() {
    var message = DirectConnectionState.schemaRegistryErrorMessage(
        new ConnectException("Connection refused"), URI
    );

    assertEquals("Unable to reach the Schema Registry URL http://localhost:8081", message);
  }
}
