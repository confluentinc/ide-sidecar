package io.confluent.idesidecar.restapi.kafkarest;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;
import java.util.stream.Stream;

public interface AvroLogicalTypesSuite extends RecordsV3BaseSuite {
  String UUID_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"string\", \"logicalType\": \"uuid\" }";
  String DATE_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"int\", \"logicalType\": \"date\" }";
  String TIME_MILLIS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"int\", \"logicalType\": \"time-millis\" }";
  String TIME_MICROS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"long\", \"logicalType\": \"time-micros\" }";
  String TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }";
  String TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"long\", \"logicalType\": \"timestamp-micros\" }";
  String LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"long\", \"logicalType\": \"local-timestamp-millis\" }";
  String LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA = "{ \"type\": \"long\", \"logicalType\": \"local-timestamp-micros\" }";

  String SAMPLE_UUID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000").toString();
  // Epoch for Mar 18 2025 is 20165, days since Unix epoch (1970-01-01)
  BigInteger SAMPLE_DATE = BigInteger.valueOf(20165);
  BigInteger SAMPLE_TIME_MILLIS = new BigInteger("400");
  BigInteger SAMPLE_TIME_MICROS = new BigInteger("900");
  BigInteger SAMPLE_TIMESTAMP_MILLIS = new BigInteger("1742345000211");
  BigInteger SAMPLE_TIMESTAMP_MICROS = new BigInteger("1742345000211000");
  BigInteger SAMPLE_LOCAL_TIMESTAMP_MILLIS = new BigInteger("1742345000211");
  BigInteger SAMPLE_LOCAL_TIMESTAMP_MICROS = new BigInteger("1742345000211000");

  static Stream<Arguments> avroLogicalTypeSchemas() {
    return Stream.of(
        Arguments.of(UUID_LOGICAL_TYPE_SCHEMA, "uuid", SAMPLE_UUID, SAMPLE_UUID),
        Arguments.of(DATE_LOGICAL_TYPE_SCHEMA, "date", SAMPLE_DATE, SAMPLE_DATE),
        Arguments.of(TIME_MILLIS_LOGICAL_TYPE_SCHEMA, "time-millis", SAMPLE_TIME_MILLIS, SAMPLE_TIME_MILLIS),
        Arguments.of(TIME_MICROS_LOGICAL_TYPE_SCHEMA, "time-micros", SAMPLE_TIME_MICROS, SAMPLE_TIME_MICROS),
        Arguments.of(TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA, "timestamp-millis", SAMPLE_TIMESTAMP_MILLIS, SAMPLE_TIMESTAMP_MILLIS),
        Arguments.of(TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA, "timestamp-micros", SAMPLE_TIMESTAMP_MICROS, SAMPLE_TIMESTAMP_MICROS),
        Arguments.of(LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA, "local-timestamp-millis", SAMPLE_LOCAL_TIMESTAMP_MILLIS, SAMPLE_LOCAL_TIMESTAMP_MILLIS),
        Arguments.of(LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA, "local-timestamp-micros", SAMPLE_LOCAL_TIMESTAMP_MICROS, SAMPLE_LOCAL_TIMESTAMP_MICROS)
    );
  }

  /**
   * TODO: All but Decimal logical type have been tested. The Decimal logical type is
   *       a wrapper of the Avro bytes type, which will require lots of byte manipulation
   *       We need to prefix with magic byte and (schema id & 0xF) and send the bytes
   *       as a base64 encoded string.
   * @param keySchema
   * @param logicalType
   * @param sampleData
   * @throws IOException
   */
  @ParameterizedTest(name = "shouldDeserializeAvroLogicalTypes: {1}")
  @MethodSource("avroLogicalTypeSchemas")
  default void shouldDeserializeAvroLogicalTypes(
      String keySchema, String logicalType, Object sampleData) throws IOException {
    var topicName = randomTopicName();
    createTopic(topicName);

    // And for the value schema, we use ALL the Avro logical types
    var valueSchema = """
      {
        "type": "record",
        "name": "LogicalTypesRecord",
        "fields": [
          {"name": "uuidField", "type": %s },
          {"name": "dateField", "type": %s },
          {"name": "timeMillisField", "type": %s },
          {"name": "timeMicrosField", "type": %s },
          {"name": "timestampMillisField", "type": %s },
          {"name": "timestampMicrosField", "type": %s },
          {"name": "localTimestampMillisField", "type": %s },
          {"name": "localTimestampMicrosField", "type": %s }
        ]
      }
      """.formatted(
        UUID_LOGICAL_TYPE_SCHEMA,
        DATE_LOGICAL_TYPE_SCHEMA,
        TIME_MILLIS_LOGICAL_TYPE_SCHEMA,
        TIME_MICROS_LOGICAL_TYPE_SCHEMA,
        TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA,
        TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA,
        LOCAL_TIMESTAMP_MILLIS_LOGICAL_TYPE_SCHEMA,
        LOCAL_TIMESTAMP_MICROS_LOGICAL_TYPE_SCHEMA
    );

    var keySubjectName = getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, true);
    var createdKeySchema = createSchema(
        keySubjectName,
        SchemaFormat.AVRO.name(),
        keySchema
    );

    var valueSubjectName = getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, false);
    var createdValueSchema = createSchema(
        valueSubjectName,
        SchemaFormat.AVRO.name(),
        valueSchema
    );

    var valueData = """
        {
          "uuidField": "%s",
          "dateField": %s,
          "timeMillisField": %s,
          "timeMicrosField": %s,
          "timestampMillisField": %s,
          "timestampMicrosField": %s,
          "localTimestampMillisField": %s,
          "localTimestampMicrosField": %s
        }
        """.formatted(
        SAMPLE_UUID,
        SAMPLE_DATE,
        SAMPLE_TIME_MILLIS,
        SAMPLE_TIME_MICROS,
        SAMPLE_TIMESTAMP_MILLIS,
        SAMPLE_TIMESTAMP_MICROS,
        SAMPLE_LOCAL_TIMESTAMP_MILLIS,
        SAMPLE_LOCAL_TIMESTAMP_MICROS
    );

    produceRecord(
        topicName,
        sampleData,
        createdKeySchema.getVersion(),
        OBJECT_MAPPER.readTree(valueData),
        createdValueSchema.getVersion()
    );

    assertTopicHasRecord(
        new RecordData(
            SchemaFormat.PROTOBUF,
            SubjectNameStrategyEnum.TOPIC_NAME,
            keySchema,
            keySubjectName,
            createdKeySchema.getId(),
            sampleData
        ),
        new RecordData(
            SchemaFormat.PROTOBUF,
            SubjectNameStrategyEnum.TOPIC_NAME,
            valueSchema,
            valueSubjectName,
            createdValueSchema.getId(),
            valueData
        ),
        topicName
    );
  }
}
