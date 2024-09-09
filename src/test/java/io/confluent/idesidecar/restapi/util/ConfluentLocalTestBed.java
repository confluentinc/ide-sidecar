package io.confluent.idesidecar.restapi.util;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.testcontainers.utility.DockerImageName;

public class ConfluentLocalTestBed {
  public static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.0");
  private final Network network;
  private final KafkaContainer kafka;
  private final SchemaRegistryContainer schemaRegistry;

  public ConfluentLocalTestBed() {
    this.network = Network.newNetwork();
    this.kafka = new KafkaContainer(KAFKA_IMAGE)
        .withNetwork(network)
        .withNetworkAliases("kafka")
        .waitingFor(Wait.forLogMessage(".*started.*\\n", 1));

    this.schemaRegistry = new SchemaRegistryContainer("kafka:9092")
        .withNetwork(network)
        .withExposedPorts(8085)
        .dependsOn(kafka)
        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
  }

  public void start() {
    kafka.start();
    schemaRegistry.start();
  }

  public void stop() {
    schemaRegistry.stop();
    kafka.stop();
  }

  public boolean isReady() {
    return isKafkaReady() && isSchemaRegistryReady();
  }

  private boolean isKafkaReady() {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
      return true;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      System.err.println("Kafka is not ready: " + e.getMessage());
      return false;
    }
  }

  private boolean isSchemaRegistryReady() {
    try {
      String endpoint = schemaRegistry.endpoint();
      java.net.HttpURLConnection connection = (java.net.HttpURLConnection) new java.net.URL(endpoint + "/subjects").openConnection();
      connection.setRequestMethod("GET");
      int responseCode = connection.getResponseCode();
      return responseCode == 200;
    } catch (java.io.IOException e) {
      System.err.println("Schema Registry is not ready: " + e.getMessage());
      return false;
    }
  }

  public void createTopic(String topicName, int partitions, short replicationFactor) throws InterruptedException, ExecutionException, TimeoutException {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
    }
  }

  public void deleteTopic(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      adminClient.deleteTopics(Collections.singleton(topicName)).all().get(30, TimeUnit.SECONDS);
    }
  }

  public void waitForTopicCreation(String topicName, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
    long startTime = System.currentTimeMillis();
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
        if (adminClient.listTopics().names().get().contains(topicName)) {
          return;
        }
        Thread.sleep(1000);
      }
      throw new TimeoutException("Timed out waiting for topic creation: " + topicName);
    }
  }

  public boolean topicExists(String topicName) throws InterruptedException, ExecutionException, TimeoutException {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      return adminClient.listTopics().names().get(30, TimeUnit.SECONDS).contains(topicName);
    }
  }

  public void waitForTopicDeletion(String topicName, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
    long startTime = System.currentTimeMillis();
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
          return;
        }
        Thread.sleep(1000);
      }
      throw new TimeoutException("Timed out waiting for topic deletion: " + topicName);
    }
  }

  public Set<String> listTopics() throws InterruptedException, ExecutionException, TimeoutException {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      ListTopicsResult topics = adminClient.listTopics();
      return topics.names().get(30, TimeUnit.SECONDS);
    }
  }

  public Properties getKafkaProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return props;
  }

  public <K, V> KafkaProducer<K, V> createProducer(Class<?> keySerializerClass, Class<?> valueSerializerClass) {
    Properties props = getKafkaProperties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    return new KafkaProducer<>(props);
  }

  public <K, V> KafkaConsumer<K, V> createConsumer(String groupId, Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
    Properties props = getKafkaProperties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(props);
  }

  // Convenience methods for creating specific types of producers and consumers
  public KafkaProducer<String, GenericRecord> createAvroProducer() {
    return createProducer(StringSerializer.class, KafkaAvroSerializer.class);
  }

  public KafkaConsumer<String, GenericRecord> createAvroConsumer(String groupId) {
    return createConsumer(groupId, StringDeserializer.class, KafkaAvroDeserializer.class);
  }

  public <T extends com.google.protobuf.Message> KafkaProducer<String, T> createProtobufProducer() {
    Properties props = getKafkaProperties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    return new KafkaProducer<>(props);
  }

  public <T extends com.google.protobuf.Message> KafkaConsumer<String, T> createProtobufConsumer(String groupId, Class<T> messageType) {
    Properties props = getKafkaProperties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, messageType.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(props);
  }

  public KafkaProducer<String, JSONObject> createJsonSchemaProducer() {
    Properties props = getKafkaProperties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    return new KafkaProducer<>(props);
  }

  public KafkaConsumer<String, Map<String, Object>> createJsonSchemaConsumer(String groupId) {
    Properties props = getKafkaProperties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.endpoint());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Map.class.getName());
    return new KafkaConsumer<>(props);
  }

  public KafkaProducer<String, String> createStringProducer() {
    return createProducer(StringSerializer.class, StringSerializer.class);
  }

  public KafkaConsumer<String, String> createStringConsumer(String groupId) {
    return createConsumer(groupId, StringDeserializer.class, StringDeserializer.class);
  }

  public String getKafkaBootstrapServers() {
    return kafka.getBootstrapServers();
  }

  public String getSchemaRegistryUrl() {
    return schemaRegistry.endpoint();
  }
}