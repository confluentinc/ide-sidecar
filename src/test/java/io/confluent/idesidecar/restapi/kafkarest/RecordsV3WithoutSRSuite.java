package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import java.util.Map;

import static io.confluent.idesidecar.restapi.kafkarest.RecordsV3Suite.SCHEMALESS_RECORD_DATA_VALUES;
import static io.confluent.idesidecar.restapi.kafkarest.RecordsV3Suite.produceAndConsume;
import static org.hamcrest.Matchers.containsString;

public interface RecordsV3WithoutSRSuite extends ITSuite {

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
  default void testProduceAndConsumeSchemalessData(RecordsV3Suite.RecordData key, RecordsV3Suite.RecordData value) {
    produceAndConsume(this, key, value);
  }
}
