package io.confluent.idesidecar.restapi.messageviewer;


import io.confluent.idesidecar.restapi.avro.MyAvroMessage;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
public class SimpleConsumerIT {

  private static ConfluentLocalTestBed testBed;
  private static SimpleConsumer simpleConsumer;

  @BeforeAll
  public static void setUp() {
    testBed = new ConfluentLocalTestBed();
    testBed.start();
    assertTrue(testBed.isReady(), "Test bed should be ready");

    Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", testBed.getBootstrapServers());
    consumerProps.setProperty("schema.registry.url", testBed.getSchemaRegistryUrl());
    simpleConsumer = new SimpleConsumer(
        consumerProps,
        new CachedSchemaRegistryClient(
            testBed.getSchemaRegistryUrl(),
            10,
            Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()),
            Collections.emptyMap()
        )
    );
  }

  @AfterAll
  public static void tearDown() {
    testBed.stop();
  }

  void recreateTopic(String topic) throws ExecutionException, InterruptedException, TimeoutException {
    if (testBed.topicExists(topic)) {
      testBed.deleteTopic(topic);
      testBed.waitForTopicCreation(topic, Duration.ofSeconds(30));
    }
    testBed.createTopic(topic, 1, (short) 1);
    testBed.waitForTopicCreation(topic, Duration.ofSeconds(30));
  }

  @Test
  void testAvroProduceAndConsume() throws ExecutionException, InterruptedException, TimeoutException {
    String topic = "myavromessage1";
    recreateTopic(topic);

    Producer<String, GenericRecord> producer = testBed.createAvroProducer();
    GenericRecordBuilder myMessageBuilder = new GenericRecordBuilder(MyAvroMessage.SCHEMA$);

    List<String> ids = Arrays.asList("12345", "12346", "12347");
    List<String> values = Arrays.asList("Test Value 1", "Test Value 2", "Test Value 3");

    for (int i = 0; i < 3; i++) {
      myMessageBuilder.set("id", ids.get(i));
      myMessageBuilder.set("value", values.get(i));
      GenericRecord myMessage = myMessageBuilder.build();

      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, "message-key-" + i, myMessage);
      producer.send(record);
    }

    producer.flush();
    producer.close();

    Map<Integer, Long> offsets = new HashMap<>();
    offsets.put(0, 0L);  // Assuming single partition, start from offset 0
    List<PartitionConsumeData> response = simpleConsumer.consumeFromMultiplePartitions(
        topic, offsets, true, null, 10, null, null);

    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.get(0);
    assertEquals(3, partitionData.records().size(), "Should have 3 records");

    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      assertEquals(ids.get(i), record.value().get("id").asText(), "ID should match");
      assertEquals(values.get(i), record.value().get("value").asText(), "Value should match");
    }
  }

  @Test
  public void testProtoProduceAndConsumeMultipleRecords() throws Exception {
    String topic = "myProtobufTopic";
    recreateTopic(topic);

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

    KafkaProducer<String, MyMessage> producer = testBed.createProtobufProducer();

    for (int i = 0; i < messages.size(); i++) {
      ProducerRecord<String, MyMessage> producerRecord = new ProducerRecord<>(topic, keys.get(i), messages.get(i));
      producer.send(producerRecord);
    }
    producer.close();

    Map<Integer, Long> offsets = new HashMap<>();
    offsets.put(0, 0L);  // Assuming single partition, start from offset 0
    var response = simpleConsumer.consumeFromMultiplePartitions(
        topic, offsets, true, null, 10, null, null);

    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.get(0);
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
  public void testJsonProducerAndConsumer() throws Exception {
    String topic = "test-json-topic";
    recreateTopic(topic);

    KafkaProducer<String, JSONObject> producer = testBed.createJsonSchemaProducer();

    List<JSONObject> sentRecords = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      JSONObject json = new JSONObject();
      json.put("id", i);
      json.put("name", "Person " + i);
      json.put("email", "person" + i + "@example.com");
      sentRecords.add(json);

      ProducerRecord<String, JSONObject> record = new ProducerRecord<>(topic, "key" + i, json);
      producer.send(record).get();
    }

    producer.close();

    Map<Integer, Long> offsets = new HashMap<>();
    offsets.put(0, 0L);  // Assuming single partition, start from offset 0
    var response = simpleConsumer.consumeFromMultiplePartitions(
        topic, offsets, true, null, 10, null, null);

    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.get(0);
    assertEquals(3, partitionData.records().size(), "Should have 3 records");

    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      JSONObject sentJson = sentRecords.get(i);
      assertEquals(sentJson.getInt("id"), record.value().get("id").asInt(), "ID should match");
      assertEquals(sentJson.getString("name"), record.value().get("name").asText(), "Name should match");
      assertEquals(sentJson.getString("email"), record.value().get("email").asText(), "Email should match");
    }
  }

  @Test
  public void testProduceAndConsumeMultipleStringRecords() throws Exception {
    String topic = "test-str-topic";
    recreateTopic(topic);

    KafkaProducer<String, String> producer = testBed.createStringProducer();

    List<ProducerRecord<String, String>> records = Arrays.asList(
        new ProducerRecord<>(topic, "key1", "value1"),
        new ProducerRecord<>(topic, "key2", "value2"),
        new ProducerRecord<>(topic, "key3", "value3")
    );

    for (ProducerRecord<String, String> record : records) {
      producer.send(record).get();
    }

    producer.close();

    Map<Integer, Long> offsets = new HashMap<>();
    offsets.put(0, 0L);  // Assuming single partition, start from offset 0
    var response = simpleConsumer.consumeFromMultiplePartitions(
        topic, offsets, true, null, 10, null, null);

    assertEquals(1, response.size(), "Should have data for 1 partition");
    PartitionConsumeData partitionData = response.get(0);
    assertEquals(3, partitionData.records().size(), "Should have 3 records");

    for (int i = 0; i < 3; i++) {
      PartitionConsumeRecord record = partitionData.records().get(i);
      String key = record.key().asText();
      String value = record.value().asText();

      assertEquals(records.get(i).key(), key, "Key should match");
      assertEquals(records.get(i).value(), value, "Value should match");
    }
  }
}