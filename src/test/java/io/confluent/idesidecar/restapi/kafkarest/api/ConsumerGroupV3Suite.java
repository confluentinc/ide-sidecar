package io.confluent.idesidecar.restapi.kafkarest.api;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Integration test suite for the Consumer Group V3 API endpoints. Tests verify that
 * consumer group data is correctly returned for LOCAL and DIRECT connections by creating
 * Kafka consumers with known group IDs and validating the REST API responses.
 */
public interface ConsumerGroupV3Suite extends ITSuite {

  String CONSUMER_GROUPS_PATH =
      "/internal/kafka/v3/clusters/{cluster_id}/consumer-groups";

  @Test
  default void shouldListConsumerGroups() {
    var topicName = "cg-list-test-" + UUID.randomUUID();
    var groupId = "test-group-list-" + UUID.randomUUID();
    createTopic(topicName);

    // create a consumer group by consuming from the topic
    consumeWithGroup(topicName, groupId);

    givenDefault()
        .get(CONSUMER_GROUPS_PATH)
        .then()
        .statusCode(200)
        .body("kind", equalTo("KafkaConsumerGroupList"))
        .body("data.find { it.consumer_group_id == '%s' }".formatted(groupId),
            notNullValue())
        .body(
            "data.find { it.consumer_group_id == '%s' }.kind".formatted(groupId),
            equalTo("KafkaConsumerGroup"));
  }

  @Test
  default void shouldGetConsumerGroup() {
    var topicName = "cg-get-test-" + UUID.randomUUID();
    var groupId = "test-group-get-" + UUID.randomUUID();
    createTopic(topicName);

    consumeWithGroup(topicName, groupId);

    givenDefault()
        .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}", groupId)
        .then()
        .statusCode(200)
        .body("kind", equalTo("KafkaConsumerGroup"))
        .body("consumer_group_id", equalTo(groupId))
        .body("state", notNullValue())
        .body("coordinator", notNullValue())
        .body("consumer.related", notNullValue())
        .body("lag_summary.related", notNullValue());
  }

  @Test
  default void shouldReturn404ForNonExistentConsumerGroup() {
    givenDefault()
        .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}",
            "non-existent-group-" + UUID.randomUUID())
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404));
  }

  @Test
  default void shouldListConsumers() {
    var topicName = "cg-consumers-test-" + UUID.randomUUID();
    var groupId = "test-group-consumers-" + UUID.randomUUID();
    createTopic(topicName);

    // keep a consumer active so we can see members
    try (var consumer = createKafkaConsumer(topicName, groupId)) {
      consumer.poll(Duration.ofSeconds(5));

      givenDefault()
          .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/consumers", groupId)
          .then()
          .statusCode(200)
          .body("kind", equalTo("KafkaConsumerList"))
          .body("data", hasSize(1))
          .body("data[0].kind", equalTo("KafkaConsumer"))
          .body("data[0].consumer_group_id", equalTo(groupId))
          .body("data[0].consumer_id", not(empty()))
          .body("data[0].assignments.related", notNullValue());
    }
  }

  @Test
  default void shouldGetConsumer() {
    var topicName = "cg-get-consumer-test-" + UUID.randomUUID();
    var groupId = "test-group-get-consumer-" + UUID.randomUUID();
    createTopic(topicName);

    try (var consumer = createKafkaConsumer(topicName, groupId)) {
      consumer.poll(Duration.ofSeconds(5));

      // first list consumers to get the consumer ID
      var consumerId = givenDefault()
          .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/consumers", groupId)
          .then()
          .statusCode(200)
          .extract()
          .path("data[0].consumer_id");

      // then get the specific consumer
      givenDefault()
          .get(CONSUMER_GROUPS_PATH
                  + "/{consumer_group_id}/consumers/{consumer_id}",
              groupId, consumerId)
          .then()
          .statusCode(200)
          .body("kind", equalTo("KafkaConsumer"))
          .body("consumer_group_id", equalTo(groupId))
          .body("consumer_id", equalTo(consumerId));
    }
  }

  @Test
  default void shouldListConsumerAssignments() {
    var topicName = "cg-assignments-test-" + UUID.randomUUID();
    var groupId = "test-group-assignments-" + UUID.randomUUID();
    createTopic(topicName);

    try (var consumer = createKafkaConsumer(topicName, groupId)) {
      consumer.poll(Duration.ofSeconds(5));

      // get the consumer ID first
      var consumerId = givenDefault()
          .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/consumers", groupId)
          .then()
          .statusCode(200)
          .extract()
          .path("data[0].consumer_id");

      // list assignments for that consumer
      givenDefault()
          .get(CONSUMER_GROUPS_PATH
                  + "/{consumer_group_id}/consumers/{consumer_id}/assignments",
              groupId, consumerId)
          .then()
          .statusCode(200)
          .body("kind", equalTo("KafkaConsumerAssignmentList"))
          .body("data", not(empty()))
          .body("data[0].kind", equalTo("KafkaConsumerAssignment"))
          .body("data[0].topic_name", equalTo(topicName))
          .body("data[0].partition.related", notNullValue())
          .body("data[0].lag.related", notNullValue());
    }
  }

  @Test
  default void shouldGetConsumerAssignment() {
    var topicName = "cg-get-assignment-test-" + UUID.randomUUID();
    var groupId = "test-group-get-assignment-" + UUID.randomUUID();
    createTopic(topicName);

    try (var consumer = createKafkaConsumer(topicName, groupId)) {
      consumer.poll(Duration.ofSeconds(5));

      var consumerId = givenDefault()
          .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/consumers", groupId)
          .then()
          .statusCode(200)
          .extract()
          .path("data[0].consumer_id");

      // get the specific assignment for partition 0
      givenDefault()
          .get(CONSUMER_GROUPS_PATH
                  + "/{consumer_group_id}/consumers/{consumer_id}"
                  + "/assignments/{topic_name}/partitions/{partition_id}",
              groupId, consumerId, topicName, 0)
          .then()
          .statusCode(200)
          .body("kind", equalTo("KafkaConsumerAssignment"))
          .body("topic_name", equalTo(topicName))
          .body("partition_id", equalTo(0))
          .body("consumer_id", equalTo(consumerId));
    }
  }

  @Test
  default void shouldGetConsumerGroupLagSummary() {
    var topicName = "cg-lag-summary-test-" + UUID.randomUUID();
    var groupId = "test-group-lag-summary-" + UUID.randomUUID();
    createTopic(topicName);

    // produce some messages
    produceStringMessages(topicName, 5);

    // consume only some, then commit and close
    consumeWithGroup(topicName, groupId);

    // produce more messages to create lag
    produceStringMessages(topicName, 3);

    givenDefault()
        .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/lag-summary", groupId)
        .then()
        .statusCode(200)
        .body("kind", equalTo("KafkaConsumerGroupLagSummary"))
        .body("consumer_group_id", equalTo(groupId))
        .body("max_lag", greaterThanOrEqualTo(0))
        .body("total_lag", greaterThanOrEqualTo(0));
  }

  @Test
  default void shouldListConsumerLags() {
    var topicName = "cg-lags-test-" + UUID.randomUUID();
    var groupId = "test-group-lags-" + UUID.randomUUID();
    createTopic(topicName);

    produceStringMessages(topicName, 3);
    consumeWithGroup(topicName, groupId);
    produceStringMessages(topicName, 2);

    givenDefault()
        .get(CONSUMER_GROUPS_PATH + "/{consumer_group_id}/lags", groupId)
        .then()
        .statusCode(200)
        .body("kind", equalTo("KafkaConsumerLagList"))
        .body("data", not(empty()))
        .body("data[0].kind", equalTo("KafkaConsumerLag"))
        .body("data[0].consumer_group_id", equalTo(groupId))
        .body("data[0].topic_name", equalTo(topicName))
        .body("data[0].current_offset", notNullValue())
        .body("data[0].log_end_offset", notNullValue())
        .body("data[0].lag", greaterThanOrEqualTo(0));
  }

  @Test
  default void shouldGetConsumerLag() {
    var topicName = "cg-get-lag-test-" + UUID.randomUUID();
    var groupId = "test-group-get-lag-" + UUID.randomUUID();
    createTopic(topicName);

    produceStringMessages(topicName, 3);
    consumeWithGroup(topicName, groupId);
    produceStringMessages(topicName, 2);

    givenDefault()
        .get(CONSUMER_GROUPS_PATH
                + "/{consumer_group_id}/lags/{topic_name}/partitions/{partition_id}",
            groupId, topicName, 0)
        .then()
        .statusCode(200)
        .body("kind", equalTo("KafkaConsumerLag"))
        .body("consumer_group_id", equalTo(groupId))
        .body("topic_name", equalTo(topicName))
        .body("partition_id", equalTo(0))
        .body("current_offset", notNullValue())
        .body("log_end_offset", notNullValue())
        .body("lag", greaterThanOrEqualTo(0));
  }

  @Test
  default void shouldRaise404OnNonExistentCluster() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/non-existent-cluster/consumer-groups")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  /**
   * Creates a Kafka consumer with the given group ID, subscribes to the topic,
   * polls for records, commits offsets, and closes — leaving committed offsets
   * so the consumer group becomes EMPTY/DEAD with stored state.
   */
  private void consumeWithGroup(String topicName, String groupId) {
    try (var consumer = createKafkaConsumer(topicName, groupId)) {
      consumer.poll(Duration.ofSeconds(5));
      consumer.commitSync();
    }
  }

  /**
   * Creates a Kafka consumer subscribed to the given topic with the specified group ID.
   * Caller is responsible for closing the consumer.
   */
  private KafkaConsumer<String, String> createKafkaConsumer(
      String topicName, String groupId
  ) {
    var props = new Properties();
    props.putAll(kafkaClientConfig());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    // speed up group join for tests
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

    var consumer = new KafkaConsumer<String, String>(props);
    consumer.subscribe(List.of(topicName));
    return consumer;
  }

  /**
   * Produces string messages to the given topic using the test environment's
   * Kafka client configuration.
   */
  private void produceStringMessages(String topicName, int count) {
    var props = new Properties();
    props.putAll(kafkaClientConfig());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    try (var producer = new KafkaProducer<String, String>(props)) {
      for (int i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topicName, "key-" + i, "value-" + i));
      }
      producer.flush();
    }
  }
}
