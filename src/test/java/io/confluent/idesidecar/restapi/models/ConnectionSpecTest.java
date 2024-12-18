package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class ConnectionSpecTest {

  record TestInput(
      String displayName,
      String resourceFilePath
  ) {
  }

  @TestFactory
  Stream<DynamicTest> testCombinations() {

    List<TestInput> inputs = List.of(
        new TestInput(
            "CCloud connection spec",
            "connections/connection-spec-ccloud.json"
        ),
        new TestInput(
            "Local connection spec",
            "connections/connection-spec-local.json"
            ),
        new TestInput(
            "Direct connection spec with Kafka and SR and mixed creds",
            "connections/connection-spec-direct-with-both-mixed.json"
        ),
        new TestInput(
            "Direct connection spec with Kafka and SR and no creds",
            "connections/connection-spec-direct-with-both-anonymous.json"
        ),
        new TestInput(
            "Direct connection spec with Kafka and no creds",
            "connections/connection-spec-direct-with-kafka-anonymous.json"
        ),
        new TestInput(
            "Direct connection spec with Kafka and basic creds",
            "connections/connection-spec-direct-with-kafka-basic.json"
        ),
        new TestInput(
            "Direct connection spec with Kafka and API key creds",
            "connections/connection-spec-direct-with-kafka-api-key.json"
        ),
        new TestInput(
            "Direct connection spec with SR and no creds",
            "connections/connection-spec-direct-with-sr-anonymous.json"
        ),
        new TestInput(
            "Direct connection spec with SR and basic creds",
            "connections/connection-spec-direct-with-sr-basic.json"
        ),
        new TestInput(
            "Direct connection spec with SR and API key creds",
            "connections/connection-spec-direct-with-sr-api-key.json"
        ),
        new TestInput(
            "Direct connection spec with MDS and no creds",
            "connections/connection-spec-mds-anonymous.json"
        ),
        new TestInput(
            "Direct connection spec with MDS and basic creds",
            "connections/connection-spec-mds-basic.json"
        )
    );
    return inputs
        .stream()
        .map(input -> DynamicTest.dynamicTest(
            "Testing: " + input.displayName,
            () -> deserializeAndSerialize(input.resourceFilePath, ConnectionSpec.class)
        ));
  }
}
