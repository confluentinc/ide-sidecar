package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.confluent.idesidecar.restapi.util.ByteArrayJsonUtil;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Optional;

/**
 * Encapsulates logic to serialize data based on the schema type. Defaults to JSON serialization if
 * no schema is provided.
 */
@ApplicationScoped
public class RecordSerializer {

  @Inject
  ClientConfigurator clientConfigurator;

  private static final ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapper();


  public ByteString serialize(
      SchemaRegistryClient client,
      Optional<SchemaManager.RegisteredSchema> schema,
      String topicName,
      Object data,
      boolean isKey
  ) {
    if (data == null) {
      return null;
    }

    try {
      var serdeConfigs = clientConfigurator.getSerdeConfigs(schema, isKey);
      if (schema.isEmpty()) {
        return serializeSchemalessData(topicName, serdeConfigs, data, isKey);
      }
      var jsonNode = objectMapper.valueToTree(data);

      // If we were sent bytes, we should just use them as-is.
      if (ByteArrayJsonUtil.smellsLikeBytes(jsonNode)) {
        return ByteString.copyFrom(ByteArrayJsonUtil.asBytes(jsonNode));
      }

      var parsedSchema = schema.get().parsedSchema();
      return switch (SchemaFormat.fromSchemaType(parsedSchema.schemaType())) {
        case AVRO -> AvroRecordSerializer.serialize(
            client, parsedSchema, serdeConfigs, topicName, jsonNode, isKey);
        case JSON ->
            serializeJsonSchema(client, parsedSchema, serdeConfigs, topicName, jsonNode, isKey);
        case PROTOBUF ->
            serializeProtobuf(client, parsedSchema, serdeConfigs, topicName, jsonNode, isKey);
      };
    } catch (RuntimeException e) {
      // Wrap the exception with key/value information in the exception message.
      var what = isKey ? "key" : "value";
      Log.errorf(e,
          "Failed to serialize %s when producing message to topic %s", what, topicName
      );
      throw new RuntimeException(
          "Failed to serialize %s when producing message to topic %s: %s"
              .formatted(what, topicName, e.getMessage()),
          e
      );
    }
  }

  private ByteString serializeJsonSchema(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      Map<String, String> configs,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var jsonschemaSerializer = new KafkaJsonSchemaSerializer<>(client)) {
      jsonschemaSerializer.configure(configs, isKey);
      var schema = (JsonSchema) parsedSchema;
      var record = wrappedToObject(() -> JsonSchemaUtils.toObject(data, schema));
      return ByteString.copyFrom(jsonschemaSerializer.serialize(topicName, record));
    }
  }

  private ByteString serializeProtobuf(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      Map<String, String> configs,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var protobufSerializer = new KafkaProtobufSerializer<>(client)) {
      protobufSerializer.configure(configs, isKey);
      var schema = (ProtobufSchema) parsedSchema;
      var typeRegistry = JsonFormat.TypeRegistry
          .newBuilder()
          .add(schema.toDescriptor())
          .build();
      var record = (Message) wrappedToObject(() -> {
        var out = new StringWriter();
        objectMapper.writeValue(out, data);
        var message = schema.newMessageBuilder();
        JsonFormat
            .parser()
            .usingTypeRegistry(typeRegistry)
            .merge(out.toString(), message);
        return message.build();
      });
      return ByteString.copyFrom(protobufSerializer.serialize(topicName, record));
    }
  }

  /**
   * Cute, eh? This is a functional interface that allows us to pass a supplier that throws a
   * checked exception.
   *
   * @param <T> The type of the object to be supplied.
   */
  @FunctionalInterface
  public interface ThrowingSupplier<T, E extends Exception> {

    T get() throws E;
  }

  public static Object wrappedToObject(ThrowingSupplier<Object, IOException> toObjectSupplier) {
    try {
      return toObjectSupplier.get();
    } catch (Exception e) {
      throw new BadRequestException(
          "Failed to parse data: %s".formatted(e.getMessage()), e);
    }
  }

  /**
   * First serialize the schemaless data as JSON using UTF-8 encoding. If the data is a single field
   * named {@code __raw__}, the value of that field is assumed to be a base64-encoded byte array and
   * is decoded and returned as a {@link ByteString}.
   */
  private ByteString serializeSchemalessData(
      String topicName,
      Map<String, ?> configs,
      Object data,
      boolean isKey
  ) {
    try (var kafkaJsonSerializer = new KafkaJsonSerializer<>()) {
      kafkaJsonSerializer.configure(configs, isKey);
      var jsonUtf8Bytes = kafkaJsonSerializer.serialize(topicName, data);

      JsonNode node;
      try {
        node = objectMapper.readTree(jsonUtf8Bytes);
      } catch (IOException e) {
        // We should never get here, since we just serialized the data.
        throw new BadRequestException("Failed to parse JSON: %s".formatted(e.getMessage()), e);
      }

      if (ByteArrayJsonUtil.smellsLikeBytes(node)) {
        return ByteString.copyFrom(ByteArrayJsonUtil.asBytes(node));
      } else {
        return ByteString.copyFrom(jsonUtf8Bytes);
      }
    }
  }
}
