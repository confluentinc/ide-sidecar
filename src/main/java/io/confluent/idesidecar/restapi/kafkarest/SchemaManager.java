package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.BadRequestException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class SchemaManager {

  public static final List<SchemaProvider> SCHEMA_PROVIDERS = Collections.unmodifiableList(
      Arrays.asList(
          SchemaFormat.AVRO.schemaProvider(),
          SchemaFormat.PROTOBUF.schemaProvider(),
          SchemaFormat.JSON.schemaProvider()
      )
  );

  private static final SubjectNameStrategyEnum DEFAULT_SUBJECT_NAME_STRATEGY =
      SubjectNameStrategyEnum.TOPIC_NAME;

  public Optional<RegisteredSchema> getSchema(
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      ProduceRequestData produceRequestData,
      boolean isKey
  ) {
    // If any of the other schema related fields are set, disallow the request
    // Note: We can implement support for various combinations of these fields as we see fit.
    if (!produceRequestIsValid(produceRequestData)) {
      throw new BadRequestException(
          "This endpoint does not support specifying "
              + "schema ID, type, schema, standalone subject or subject name strategy."
      );
    }

    // If only the schemaVersion is set, use it to fetch the schema
    // with the default subject name strategy (TopicNameStrategy)
    if (onlySchemaVersion(produceRequestData)) {
      ensureSchemaRegistryClientExists(schemaRegistryClient);
      return Optional.of(getSchemaFromSchemaVersion(
          schemaRegistryClient,
          topicName,
          produceRequestData.getSchemaVersion(),
          isKey
      ));
    }

    if (schemaVersionWithSubjectAndSubjectNameStrategy(produceRequestData)) {
      ensureSchemaRegistryClientExists(schemaRegistryClient);
      return Optional.of(getSchemaFromSubject(
          schemaRegistryClient,
          produceRequestData.getSchemaVersion(),
          produceRequestData.getSubject(),
          produceRequestData.getSubjectNameStrategy()
      ));
    }

    return Optional.empty();
  }

  private static void ensureSchemaRegistryClientExists(SchemaRegistryClient schemaRegistryClient) {
    if (schemaRegistryClient == null) {
      throw new BadRequestException(
          "This connection does not have an associated Schema Registry. " +
              "Either define a Schema Registry for this connection or try again without specifying "
              +
              "schema details: schema version, subject or subject name strategy.");
    }
  }

  /**
   * Check if the ProduceRequestData contains a non-null schemaVersion and all other schema related
   * fields are null.
   */
  private static boolean onlySchemaVersion(ProduceRequestData produceRequestData) {
    // Only schemaVersion must be set
    return produceRequestData.getSchemaVersion() != null
        && produceRequestData.getSubject() == null
        && produceRequestData.getSubjectNameStrategy() == null;
  }

  private static boolean schemaVersionWithSubjectAndSubjectNameStrategy(
      ProduceRequestData produceRequestData
  ) {
    return produceRequestData.getSchemaVersion() != null
        && produceRequestData.getSubject() != null
        && produceRequestData.getSubjectNameStrategy() != null;
  }

  private static boolean produceRequestIsValid(ProduceRequestData produceRequestData) {
    // schema_id, type and schema must not be set
    var unsupportedFields = Stream.of(
        produceRequestData.getSchemaId(),
        produceRequestData.getType(),
        produceRequestData.getSchema()
    );
    // All must be null
    var areUnsupportedFieldsSet = unsupportedFields.allMatch(Objects::isNull);

    // Subject and subject name strategy must be set together, or not at all
    var subjectAndSubjectNameStrategy = Stream.of(
        produceRequestData.getSubject(),
        produceRequestData.getSubjectNameStrategy()
    ).toList();
    var validSubject = subjectAndSubjectNameStrategy.stream().allMatch(Objects::isNull)
        || subjectAndSubjectNameStrategy.stream().noneMatch(Objects::isNull);

    return areUnsupportedFieldsSet && validSubject;
  }

  private RegisteredSchema getSchemaFromSchemaVersion(
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      Integer schemaVersion,
      boolean isKey
  ) {
    var schema = schemaRegistryClient.getByVersion(
        DEFAULT_SUBJECT_NAME_STRATEGY.subjectName(topicName, isKey, null),
        schemaVersion,
        // do not lookup deleted schemas
        false
    );

    var parsedSchema = getParsedSchema(schema);

    return new RegisteredSchema(
        schema.getSubject(),
        DEFAULT_SUBJECT_NAME_STRATEGY,
        schema.getId(),
        schema.getVersion(),
        parsedSchema
    );
  }

  private RegisteredSchema getSchemaFromSubject(
      SchemaRegistryClient schemaRegistryClient,
      Integer schemaVersion,
      String subject,
      String subjectNameStrategy
  ) {
    var strategy = SubjectNameStrategyEnum.parse(subjectNameStrategy);
    if (strategy.isEmpty()) {
      throw new BadRequestException(
          "Invalid subject name strategy: %s".formatted(subjectNameStrategy)
      );
    }

    var schema = schemaRegistryClient.getByVersion(
        subject,
        schemaVersion,
        false
    );

    var parsedSchema = getParsedSchema(schema);

    return new RegisteredSchema(
        schema.getSubject(),
        strategy.get(),
        schema.getId(),
        schema.getVersion(),
        parsedSchema
    );
  }

  private ParsedSchema getParsedSchema(Schema schema) {
    var schemaFormat = SchemaFormat.fromSchemaType(schema.getSchemaType());
    var schemaProvider = Optional
        .ofNullable(schemaFormat.schemaProvider())
        .orElseThrow(() ->
            new BadRequestException("Unsupported schema type: " + schema.getSchemaType()));
    return schemaProvider
        .parseSchema(schema, false)
        .orElseThrow(() -> new BadRequestException("Failed to parse schema"));
  }


  public record RegisteredSchema(
      String subject,
      SubjectNameStrategyEnum subjectNameStrategy,
      Integer schemaId,
      Integer schemaVersion,
      ParsedSchema parsedSchema
  ) {

    public SchemaFormat type() {
      return SchemaFormat.fromSchemaType(parsedSchema.schemaType());
    }
  }
}
