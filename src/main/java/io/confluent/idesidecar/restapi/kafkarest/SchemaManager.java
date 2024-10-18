package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.BadRequestException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class SchemaManager {
  public static final List<SchemaProvider> SCHEMA_PROVIDERS = Collections.unmodifiableList(
      Arrays.asList(
        SchemaFormat.AVRO.schemaProvider(),
        SchemaFormat.PROTOBUF.schemaProvider(),
        SchemaFormat.JSON.schemaProvider()
      )
  );

  public @Nullable RegisteredSchema getSchema(
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      ProduceRequestData produceRequestData,
      boolean isKey
  ) {
    // If schemaVersion is set, use it to fetch the schema
    // We don't require the subject to be passed because we use the default TopicNameStrategy
    // as the subject name strategy. We may choose to support non-default subject name strategies
    // and that is left as a future enhancement.
    if (supportsSchemaVersion(produceRequestData)) {
      return getSchemaFromSchemaVersion(
          schemaRegistryClient,
          topicName,
          produceRequestData.getSchemaVersion(),
          isKey
      );
    }

    // If any of the other schema related fields are set, disallow the request
    // Note: We can implement support for various combinations of these fields as we see fit.
    if (unsupportedFieldsSet(produceRequestData)) {
      throw new UnsupportedOperationException(
          "This endpoint does not yet support specifying "
              + "schema ID, subject, subject name strategy, type, or schema."
      );
    }

    return null;
  }

  /**
   * Check if the ProduceRequestData contains a non-null schemaVersion and all other
   * schema related fields are null.
   */
  private static boolean supportsSchemaVersion(ProduceRequestData produceRequestData) {
    // Only schemaVersion must be set
    return produceRequestData.getSchemaVersion() != null
        && !unsupportedFieldsSet(produceRequestData);
  }

  @SuppressWarnings("BooleanExpressionComplexity")
  private static boolean unsupportedFieldsSet(ProduceRequestData produceRequestData) {
    return (produceRequestData.getSchemaId() != null
        || produceRequestData.getSubject() != null)
        || produceRequestData.getSubjectNameStrategy() != null
        || produceRequestData.getType() != null
        || produceRequestData.getSchema() != null;
  }

  private RegisteredSchema getSchemaFromSchemaVersion(
      @Nullable SchemaRegistryClient schemaRegistryClient,
      String topicName,
      Integer schemaVersion,
      boolean isKey
  ) {
    if (schemaRegistryClient == null) {
      if (schemaVersion != null) {
        throw new BadRequestException("Schema version requested without a schema registry client");
      }
      return null;
    }

    var schema = schemaRegistryClient.getByVersion(
        // Note: We default to TopicNameStrategy for the subject name for the sake of simplicity.
        (isKey ? topicName + "-key" : topicName + "-value"),
        schemaVersion,
        // do not lookup deleted schemas
        false
    );
    return new RegisteredSchema(
        schema.getSubject(), schema.getId(), schema.getVersion(), parseSchema(schema)
    );
  }

  private ParsedSchema parseSchema(Schema schema) {
    var schemaFormat = SchemaFormat.fromSchemaType(schema.getSchemaType());
    var schemaProvider = Optional
        .ofNullable(schemaFormat.schemaProvider())
        .orElseThrow(() ->
            new IllegalArgumentException("Schema type has no provider: " + schema.getSchemaType()));
    return schemaProvider
        .parseSchema(schema, false)
        .orElseThrow(() -> new BadRequestException("Failed to parse schema"));
  }

  public enum SchemaFormat {
    AVRO(new AvroSchemaProvider()),
    PROTOBUF(new ProtobufSchemaProvider()),
    JSON(new JsonSchemaProvider());

    private final SchemaProvider schemaProvider;

    SchemaFormat(SchemaProvider schemaProvider) {
      this.schemaProvider = schemaProvider;
    }

    SchemaProvider schemaProvider() {
      return schemaProvider;
    }

    /**
     * Get the SchemaFormat for the given schema type. Only formats with a schema provider are
     * supported.
     *
     * @param schemaType the schema type
     * @return the SchemaFormat
     */
    static SchemaFormat fromSchemaType(String schemaType) {
      return Arrays.stream(values())
          .filter(format -> format.schemaProvider() != null)
          .filter(format -> format.name().equalsIgnoreCase(schemaType))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Illegal schema type: " + schemaType));
    }
  }

  public record RegisteredSchema(
      String subject,
      Integer schemaId,
      Integer schemaVersion,
      ParsedSchema parsedSchema
  ) {
  }
}
