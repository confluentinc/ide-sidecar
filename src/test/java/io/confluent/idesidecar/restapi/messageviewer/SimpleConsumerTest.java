package io.confluent.idesidecar.restapi.messageviewer;


import io.confluent.idesidecar.restapi.avro.MyAvroMessage;
import io.confluent.idesidecar.restapi.proto.Message.MyMessage;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SimpleConsumerTest {

  private static ConfluentLocalTestBed testBed;
  @BeforeAll
  public static void setUp() throws InterruptedException, ExecutionException, TimeoutException {
    testBed = new ConfluentLocalTestBed();
    testBed.start();
    assertTrue(testBed.isReady(), "Test bed should be ready");
  }

  @AfterAll
  public static void tearDown() throws InterruptedException, ExecutionException, TimeoutException {
    testBed.stop();
  }

  void recreateTopic(String topic)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (testBed.topicExists(topic)) {
      testBed.deleteTopic(topic);
      testBed.waitForTopicCreation(topic, Duration.ofSeconds(30));
    }
    testBed.createTopic(topic, 1, (short) 1);
    testBed.waitForTopicCreation(topic, Duration.ofSeconds(30));
  }

  @Test
  void testAvroProduceAndConsume()
      throws ExecutionException, InterruptedException, TimeoutException {
    String topic = "myavromessage1";
    recreateTopic(topic);

    // Create the producer
    Producer<String, GenericRecord> producer = testBed.createAvroProducer();

    // Avro schema
    GenericRecordBuilder myMessageBuilder = new GenericRecordBuilder(MyAvroMessage.SCHEMA$);

    // Prepare data for 3 records
    List<String> ids = Arrays.asList("12345", "12346", "12347");
    List<String> values = Arrays.asList("Test Value 1", "Test Value 2", "Test Value 3");

    // Send 3 records
    for (int i = 0; i < 3; i++) {
      myMessageBuilder.set("id", ids.get(i));
      myMessageBuilder.set("value", values.get(i));
      GenericRecord myMessage = myMessageBuilder.build();

      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, "message-key-" + i, myMessage);
      producer.send(record);
    }

    producer.flush();
    producer.close();

    // Create the consumer
    Consumer<String, GenericRecord> consumer = testBed.createAvroConsumer("gp3");
    consumer.subscribe(Collections.singletonList(topic));

    // Poll for new data
    boolean messagesFound = false;
    int recordCount = 0;

    while (!messagesFound) {
      ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
      for (var record : records) {
        GenericRecord message = record.value();
        String id = message.get("id").toString();
        String value = message.get("value").toString();

        // Assert that the received message matches one of the sent messages
        assertEquals(ids.get(recordCount), id, "ID should match");
        assertEquals(values.get(recordCount), value, "Value should match");

        recordCount++;

        if (recordCount == 3) {
          messagesFound = true;
          break;
        }
      }
    }
    consumer.close();
  }

  @Test
  public void testProtoProduceAndConsumeMultipleRecords() throws Exception {
    String topic = "myProtobufTopic";
    recreateTopic(topic);

    // Create multiple Protobuf messages
    MyMessage message1 = MyMessage.newBuilder()
        .setName("Ravi Inguva")
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

    // Create a Kafka producer
    KafkaProducer<String, MyMessage> producer = testBed.createProtobufProducer();

    // Send the messages to Kafka
    for (int i = 0; i < messages.size(); i++) {
      ProducerRecord<String, MyMessage> producerRecord = new ProducerRecord<>(topic, keys.get(i), messages.get(i));
      producer.send(producerRecord);
    }
    producer.close();

    // Create a Kafka consumer
    KafkaConsumer<String, MyMessage> consumer = testBed.createProtobufConsumer("test-group", MyMessage.class);
    consumer.subscribe(Collections.singletonList(topic));

    // Poll for messages
    ConsumerRecords<String, MyMessage> records = consumer.poll(Duration.ofSeconds(5));

    // Assert that at least 3 messages were consumed
    assertTrue(records.count() >= 3, "Less than 3 messages were consumed");

    // Check the consumed messages
    List<String> consumedKeys = new ArrayList<>();
    List<MyMessage> consumedMessages = new ArrayList<>();

    for (ConsumerRecord<String, MyMessage> record : records) {
      consumedKeys.add(record.key());
      consumedMessages.add(record.value());
    }

    // Assert that the keys match the sent keys
    assertTrue(consumedKeys.containsAll(keys), "Not all keys were consumed as expected");

    // Assert that the messages match the sent messages
    for (int i = 0; i < messages.size(); i++) {
      MyMessage producedMessage = messages.get(i);
      assertTrue(consumedMessages.contains(producedMessage), "Message " + producedMessage + " was not consumed as expected");

      // Individual field assertions for additional robustness
      MyMessage consumedMessage = consumedMessages.get(consumedKeys.indexOf(keys.get(i)));
      assertEquals(producedMessage.getName(), consumedMessage.getName(), "The name in the consumed message is incorrect for key " + keys.get(i));
      assertEquals(producedMessage.getAge(), consumedMessage.getAge(), "The age in the consumed message is incorrect for key " + keys.get(i));
      assertEquals(producedMessage.getIsActive(), consumedMessage.getIsActive(), "The isActive field in the consumed message is incorrect for key " + keys.get(i));
    }
    consumer.close();
  }

  @Test
  public void testJsonProducerAndConsumer() throws Exception {
    String topic = "test-json-topic";
    recreateTopic(topic);

    // Create a Kafka producer
    KafkaProducer<String, JSONObject> producer = testBed.createJsonSchemaProducer();

    // Create and send 3 records
    List<JSONObject> sentRecords = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      JSONObject json = new JSONObject();
      json.put("id", i);
      json.put("name", "Person " + i);
      json.put("email", "person" + i + "@example.com");
      sentRecords.add(json);

      ProducerRecord<String, JSONObject> record = new ProducerRecord<>(topic, "key" + i, json);
      RecordMetadata metadata = producer.send(record).get();

      assertNotNull(metadata, "Record metadata should not be null");
      assertEquals(topic, metadata.topic(), "Topic should match");
      assertTrue(metadata.hasOffset(), "Record should have an offset");
    }

    producer.close();

    // Create a Kafka consumer
    KafkaConsumer<String, Map<String, Object>> consumer = testBed.createJsonSchemaConsumer("json-consumer-group");
    consumer.subscribe(Collections.singletonList(topic));

    // Consume and verify records
    List<JSONObject> receivedRecords = new ArrayList<>();
    int emptyPollCount = 0;
    while (receivedRecords.size() < 3 && emptyPollCount < 10) {
      ConsumerRecords<String, Map<String, Object>> records = consumer.poll(Duration.ofMillis(100));
      if (records.isEmpty()) {
        emptyPollCount++;
      } else {
        for (ConsumerRecord<String, Map<String, Object>> record : records) {
          JSONObject jsonObject = new JSONObject(record.value());
          receivedRecords.add(jsonObject);
          // System.out.printf("Received message: key = %s, value = %s%n", record.key(), jsonObject);
        }
      }
    }

    consumer.close();

    // Assertions
    assertEquals(3, receivedRecords.size(), "Should receive 3 records");
    for (int i = 0; i < 3; i++) {
      JSONObject sent = sentRecords.get(i);
      JSONObject received = receivedRecords.get(i);
      assertEquals(sent.getInt("id"), received.getInt("id"), "ID should match");
      assertEquals(sent.getString("name"), received.getString("name"), "Name should match");
      assertEquals(sent.getString("email"), received.getString("email"), "Email should match");
    }
  }

  @Test
  public void testProduceAndConsumeMultipleStringRecords() throws Exception {
    String topic = "test-str-topic";

    // Create Kafka producer and consumer
    try (KafkaProducer<String, String> producer = testBed.createStringProducer();
        KafkaConsumer<String, String> consumer = testBed.createStringConsumer("string-consumer-group")) {

      // Produce 3 messages
      List<ProducerRecord<String, String>> records = Arrays.asList(
          new ProducerRecord<>(topic, "key1", "value1"),
          new ProducerRecord<>(topic, "key2", "value2"),
          new ProducerRecord<>(topic, "key3", "value3")
      );

      List<RecordMetadata> metadataList = new ArrayList<>();

      // Send all records and collect metadata
      for (ProducerRecord<String, String> record : records) {
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        metadataList.add(metadata);
        /*System.out.printf("Message sent successfully: Topic: %s, Partition: %d, Offset: %d%n",
            metadata.topic(), metadata.partition(), metadata.offset());*/
      }

      // Assert that 3 records were sent
      assertEquals(3, metadataList.size(), "Expected 3 records to be sent");

      // Subscribe consumer to the topic
      consumer.subscribe(Collections.singletonList(topic));

      // Poll for messages
      Set<String> consumedKeys = new HashSet<>();
      long startTime = System.currentTimeMillis();
      long timeout = 10000; // 10 seconds timeout

      while (consumedKeys.size() < 3 && (System.currentTimeMillis() - startTime < timeout)) {
        ConsumerRecords<String, String> consumedRecords = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> consumedRecord : consumedRecords) {
          /*System.out.printf("Consumed record with key: %s, value: %s, partition: %d, offset: %d%n",
              consumedRecord.key(), consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset());*/

          consumedKeys.add(consumedRecord.key());

          // Assert that the consumed record matches one of the produced records
          assertTrue(records.stream().anyMatch(r ->
                  r.key().equals(consumedRecord.key()) && r.value().equals(consumedRecord.value())),
              "Consumed record does not match any produced record");
        }
      }

      // Assert that all 3 records were consumed
      assertEquals(3, consumedKeys.size(), "Failed to consume all 3 produced messages within the timeout period");

      // Assert that all expected keys were consumed
      assertTrue(consumedKeys.containsAll(Arrays.asList("key1", "key2", "key3")),
          "Not all expected keys were consumed");

    } catch (Exception e) {
      e.printStackTrace();
      assertFalse(true, "Exception thrown");
    }
  }

}