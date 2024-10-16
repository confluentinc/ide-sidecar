package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
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

@ApplicationScoped
public class RecordSerializer {
  static final ObjectMapper objectMapper = new ObjectMapper();

  public ByteString serialize(
      SchemaRegistryClient client,
      SchemaManager.SchemaFormat format,
      ParsedSchema parsedSchema,
      String topicName,
      Object data
  ) {
    if (data == null) {
      return ByteString.EMPTY;
    }

    if (parsedSchema == null) {
      return ByteString.copyFrom(data.toString().getBytes());
    }

    return switch (format) {
      case AVRO -> serializeAvro(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data)
      );
      case JSONSCHEMA -> serializeJsonSchema(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data)
      );
      case PROTOBUF -> serializeProtobuf(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data)
      );
      case JSON -> serializeJson(topicName, data);
    };
  }

  private ByteString serializeAvro(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data
  ) {
    try (var avroSerializer = new KafkaAvroSerializer(client)) {
      AvroSchema schema = (AvroSchema) parsedSchema;
      Object record;
      try {
        record = AvroSchemaUtils.toObject(data, schema);
      } catch (Exception e) {
        throw new BadRequestException("Failed to parse Avro data", e);
      }

      return ByteString.copyFrom(avroSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeJsonSchema(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data
  ) {
    try (var jsonschemaSerializer = new KafkaJsonSchemaSerializer<>(client)) {
      JsonSchema schema = (JsonSchema) parsedSchema;
      Object record;
      try {
        record = JsonSchemaUtils.toObject(data, schema);
      } catch (Exception e) {
        throw new BadRequestException("Failed to parse JSON data", e);
      }

      return ByteString.copyFrom(jsonschemaSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeProtobuf(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      String topicName,
      JsonNode data
  ) {
    try (var protobufSerializer = new KafkaProtobufSerializer<>(client)) {
      ProtobufSchema schema = (ProtobufSchema) parsedSchema;
      Message record;
      try {
        record = (Message) ProtobufSchemaUtils.toObject(data, schema);
      } catch (Exception e) {
        throw new BadRequestException("Failed to parse Protobuf data", e);
      }

      return ByteString.copyFrom(protobufSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeJson(String topicName, Object data) {
    try (var kafkaJsonSerializer = new KafkaJsonSerializer<>()) {
      return ByteString.copyFrom(kafkaJsonSerializer.serialize(topicName, data));
    }
  }
}