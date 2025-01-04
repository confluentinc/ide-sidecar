package io.confluent.idesidecar.restapi.kafkarest;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

public interface RecordsV3WithoutSRSuite extends RecordsV3BaseSuite {

  @Test
  default void shouldHandleProducingWithSchemaDetailsToConnectionWithoutSchemaRegistry() {
    var topic = randomTopicName();
    createTopic(topic);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(
                null,
                topic,
                Map.of(),
                // Should trigger 400
                // due to the lack of schema registry
                1,
                Map.of(),
                null,
                Set.of()
            )
                .statusCode(400)
                .body("message", containsString("This connection does not have an associated Schema Registry."))
        );
  }

  static ArgumentSets validSchemalessKeysAndValues() {
    return ArgumentSets
        .argumentsForFirstParameter(SCHEMALESS_RECORD_DATA_VALUES)
        .argumentsForNextParameter(SCHEMALESS_RECORD_DATA_VALUES);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("validSchemalessKeysAndValues")
  default void testProduceAndConsumeSchemalessData(RecordData key, RecordData value) {
    produceAndConsume(key, value);
  }
}
