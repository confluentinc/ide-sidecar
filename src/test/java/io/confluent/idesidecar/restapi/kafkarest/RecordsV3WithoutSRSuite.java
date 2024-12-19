package io.confluent.idesidecar.restapi.kafkarest;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.util.Map;
import static org.hamcrest.Matchers.containsString;

public interface RecordsV3WithoutSRSuite extends RecordsV3BaseSuite {

  @Test
  default void shouldHandleProducingWithSchemaDetailsToConnectionWithoutSchemaRegistry() {
    var topic = randomTopicName();
    createTopic(topic);

    produceRecordThen(
        null,
        topic,
        Map.of(),
        // Should trigger 400
        // due to the lack of schema registry
        1,
        Map.of(),
        null
    )
        .statusCode(400)
        .body("message", containsString("This connection does not have an associated Schema Registry."));
  }

  static ArgumentSets validSchemalessKeysAndValues() {
    return ArgumentSets
        .argumentsForFirstParameter(SCHEMALESS_RECORD_DATA_VALUES)
        .argumentsForNextParameter(SCHEMALESS_RECORD_DATA_VALUES);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("validSchemalessKeysAndValues")
  @RetryingTest(3)
  default void testProduceAndConsumeSchemalessData(RecordData key, RecordData value) {
    produceAndConsume(key, value);
  }
}
