package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.idesidecar.restapi.util.ConfigUtil;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.BadRequestException;
import java.io.IOException;
import java.util.Map;

/**
 * Encapsulates logic to serialize data based on the schema type. Defaults to JSON serialization
 * if no schema is provided.
 */
@ApplicationScoped
public class RecordSerializer {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Map<String, String> SERDE_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.serde-configs");

  public ByteString serialize(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      Object data,
      boolean isKey
  ) {
    if (data == null) {
      return null;
    }

    if (parsedSchema == null) {
      return serializeJson(topicName, data, isKey);
    }

    var jsonNode = objectMapper.valueToTree(data);
    return switch (SchemaFormat.fromSchemaType(parsedSchema.schemaType())) {
      case AVRO -> serializeAvro(client, parsedSchema, topicName, jsonNode, isKey);
      case JSON -> serializeJsonSchema(client, parsedSchema, topicName, jsonNode, isKey);
      case PROTOBUF -> serializeProtobuf(client, parsedSchema, topicName, jsonNode, isKey);
    };
  }

  private ByteString serializeAvro(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var avroSerializer = new KafkaAvroSerializer(client)) {
      avroSerializer.configure(SERDE_CONFIGS, isKey);
      var schema = (AvroSchema) parsedSchema;
      var record = wrappedToObject(() -> AvroSchemaUtils.toObject(data, schema));
      return ByteString.copyFrom(avroSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeJsonSchema(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var jsonschemaSerializer = new KafkaJsonSchemaSerializer<>(client)) {
      jsonschemaSerializer.configure(SERDE_CONFIGS, isKey);
      var schema = (JsonSchema) parsedSchema;
      var record = wrappedToObject(() -> JsonSchemaUtils.toObject(data, schema));
      return ByteString.copyFrom(jsonschemaSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeProtobuf(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var protobufSerializer = new KafkaProtobufSerializer<>(client)) {
      protobufSerializer.configure(SERDE_CONFIGS, isKey);
      var schema = (ProtobufSchema) parsedSchema;
      var record = (Message) wrappedToObject(() -> ProtobufSchemaUtils.toObject(data, schema));
      return ByteString.copyFrom(protobufSerializer.serialize(topicName, record));
    }
  }

  /**
   * Cute, eh? This is a functional interface that allows us to pass a supplier that throws a
   * checked exception.
   * @param <T> The type of the object to be supplied.
   */
  @FunctionalInterface
  public interface ThrowingSupplier<T, E extends Exception> {
    T get() throws E;
  }

  private Object wrappedToObject(ThrowingSupplier<Object, IOException> toObjectSupplier) {
    try {
      return toObjectSupplier.get();
    } catch (Exception e) {
      throw new BadRequestException(
          "Failed to parse data: %s".formatted(e.getMessage()), e);
    }
  }

  private ByteString serializeJson(String topicName, Object data, boolean isKey) {
    try (var kafkaJsonSerializer = new KafkaJsonSerializer<>()) {
      kafkaJsonSerializer.configure(SERDE_CONFIGS, isKey);
      return ByteString.copyFrom(kafkaJsonSerializer.serialize(topicName, data));
    }
  }
}