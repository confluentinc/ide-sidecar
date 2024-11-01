package io.confluent.idesidecar.restapi.messageviewer;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.proto.Message.MyMessage;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.AbstractSidecarIT;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class SimpleConsumerIT extends AbstractSidecarIT {

  @BeforeEach
  public void beforeEach() {
    setupLocalConnection(SimpleConsumerIT.class);
  }

  @Test
  void testAvroProduceAndConsume() {
    // When we create a topic
    var topic = randomTopicName();
    createTopic(topic);

    // and register a schema
    var valueSchemaVersion = createSchema(
        "%s-value".formatted(topic),
        "AVRO",
        loadResource("avro/myavromessage.avsc")
    ).getVersion();

    var ids = Arrays.asList("12345", "12346", "12347");
    var values = Arrays.asList("Test Value 1", "Test Value 2", "Test Value 3");

    // and write the records to Kafka
    for (int i = 0; i < 3; i++) {
      produceRecord(
          topic,
          "message-key-" + i,
          null,
          Map.of("id", ids.get(i), "value", values.get(i)),
          valueSchemaVersion
      );
    }

    // Then we can consume the same records
    List<PartitionConsumeData> response = simpleConsumer().consume(
        topic,
        consumeRequestSinglePartitionFromOffsetZero()
    );
    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.getFirst();
    assertEquals(3, partitionData.records().size(), "Should have 3 records");
    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      assertEquals(ids.get(i), record.value().get("id").asText(), "ID should match");
      assertEquals(values.get(i), record.value().get("value").asText(), "Value should match");
    }
  }

  @Test
  public void testProtoProduceAndConsumeMultipleRecords() {
    // When we create a topic
    var topic = randomTopicName();
    createTopic(topic);

    // And register a schema
    var valueSchemaVersion = createSchema(
        "%s-value".formatted(topic),
        "PROTOBUF",
        loadResource("proto/message.proto")
    ).getVersion();

    // Then we can create records
    MyMessage message1 = MyMessage.newBuilder()
        .setName("Some One")
        .setAge(30)
        .setIsActive(true)
        .build();

    MyMessage message2 = MyMessage.newBuilder()
        .setName("John Doe")
        .setAge(25)
        .setIsActive(false)
        .build();

    MyMessage message3 = MyMessage.newBuilder()
        .setName("Jane Smith")
        .setAge(40)
        .setIsActive(true)
        .build();

    List<MyMessage> messages = List.of(message1, message2, message3);
    List<String> keys = List.of("key1", "key2", "key3");

    // and write the records to Kafka
    for (int i = 0; i < messages.size(); i++) {
      produceRecord(
          topic,
          keys.get(i),
          null,
          Map.of(
              "name", messages.get(i).getName(),
              "age", messages.get(i).getAge(),
              "is_active", messages.get(i).getIsActive()
          ),
          valueSchemaVersion
      );
    }

    // Then we can consume the same records
    var response = simpleConsumer().consume(topic, consumeRequestSinglePartitionFromOffsetZero());
    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.getFirst();
    assertEquals(3, partitionData.records().size(), "Should have 3 records");
    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      MyMessage originalMessage = messages.get(i);
      assertEquals(originalMessage.getName(), record.value().get("name").asText(), "Name should match");
      assertEquals(originalMessage.getAge(), record.value().get("age").asInt(), "Age should match");
      assertEquals(originalMessage.getIsActive(), record.value().get("is_active").asBoolean(), "IsActive should match");
    }
  }

  @Test
  public void testJsonProducerAndConsumer() {
    // When we create a topic
    String topic = randomTopicName();
    createTopic(topic);

    record Person(int id, String name, String email) {
    }

    // And write records to Kafka
    var persons = new ArrayList<Person>();
    for (int i = 1; i <= 3; i++) {
      var person = new Person(i, "Person " + i, "person" + i + "@example.com");
      persons.add(person);
      produceRecord(
          topic,
          "key" + i,
          null,
          Map.of("id", person.id(), "name", person.name(), "email", person.email()),
          null
      );
    }

    // Then we can consume the same records
    var response = simpleConsumer().consume(topic, consumeRequestSinglePartitionFromOffsetZero());
    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.getFirst();
    assertEquals(3, partitionData.records().size(), "Should have 3 records");
    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      var sentJson = persons.get(i);
      assertEquals(sentJson.id(), record.value().get("id").asInt(), "ID should match");
      assertEquals(sentJson.name(), record.value().get("name").asText(), "Name should match");
      assertEquals(sentJson.email(), record.value().get("email").asText(), "Email should match");
    }
  }

  @Test
  public void testProduceAndConsumeMultipleStringRecords() {
    // When we create a topic
    String topic = randomTopicName();
    createTopic(topic);

    // And write records to Kafka
    var records = new String[][]{
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"}
    };
    produceStringRecords(topic, records);

    // Then we can consume the same records
    var response = simpleConsumer().consume(topic, consumeRequestSinglePartitionFromOffsetZero());

    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.getFirst();
    assertEquals(3, partitionData.records().size(), "Should have 3 records");

    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      String key = record.key().asText();
      String value = record.value().asText();

      assertEquals(records[i][0], key, "Key should match");
      assertEquals(records[i][1], value, "Value should match");
    }
  }

  private static SimpleConsumeMultiPartitionRequest consumeRequestSinglePartitionFromOffsetZero() {
    return SimpleConsumeMultiPartitionRequestBuilder
        .builder()
        .partitionOffsets(
            Collections.singletonList(
                // Assuming single partition, start from offset 0
                new SimpleConsumeMultiPartitionRequest.PartitionOffset(0, 0L)
            )
        )
        .build();
  }
}
