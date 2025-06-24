package io.confluent.idesidecar.restapi.connections;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ConnectionStatesTest {

  @Test
  void fromShouldCreateCCloudConnectionIfTypeEqualsCCloud() {
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("id", "name", ConnectionType.CCLOUD),
        null
    );

    assertInstanceOf(CCloudConnectionState.class, connectionState);
  }

  @Test
  void fromShouldCreateLocalConnectionIfTypeEqualsLocal() {
    var connectionState = ConnectionStates.from(
        new ConnectionSpec("id", "name", ConnectionType.LOCAL),
        null
    );

    assertInstanceOf(LocalConnectionState.class, connectionState);
  }
}
