package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION;

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
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;


@ApplicationScoped
public class RecordSerializer {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

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

    return switch (SchemaManager.SchemaFormat.fromSchemaType(parsedSchema.schemaType())) {
      case AVRO -> serializeAvro(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data),
          isKey
      );
      case JSON -> serializeJsonSchema(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data),
          isKey
      );
      case PROTOBUF -> serializeProtobuf(
          client,
          parsedSchema,
          topicName,
          objectMapper.valueToTree(data),
          isKey
      );
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
      avroSerializer.configure(getSchemaSerdeConfig(), isKey);
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
      JsonNode data,
      boolean isKey
  ) {
    try (var jsonschemaSerializer = new KafkaJsonSchemaSerializer<>(client)) {
      jsonschemaSerializer.configure(getSchemaSerdeConfig(), isKey);
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
      JsonNode data,
      boolean isKey
  ) {
    try (var protobufSerializer = new KafkaProtobufSerializer<>(client)) {
      protobufSerializer.configure(getSchemaSerdeConfig(), isKey);
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

  private ByteString serializeJson(String topicName, Object data, boolean isKey) {
    try (var kafkaJsonSerializer = new KafkaJsonSerializer<>()) {
      kafkaJsonSerializer.configure(getSchemaSerdeConfig(), isKey);
      return ByteString.copyFrom(kafkaJsonSerializer.serialize(topicName, data));
    }
  }

  private Map<String, ?> getSchemaSerdeConfig() {
    return Map.of(
        // The schema.registry.url is a required config, however, we don't expect
        // the serializers to actually use it to construct a new client,
        // since we pass the SchemaRegistryClient instance to them directly.
        SCHEMA_REGISTRY_URL_CONFIG, sidecarHost,
        // Disable auto-registering schemas, as we expect the schema to be registered
        // before the data is serialized.
        AUTO_REGISTER_SCHEMAS, false,
        // Do not try to fetch the latest version of the schema from the registry
        USE_LATEST_VERSION, false
    );
  }
}