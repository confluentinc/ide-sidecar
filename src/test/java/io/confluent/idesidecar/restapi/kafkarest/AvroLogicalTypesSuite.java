package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.avro.RecordWithDecimalLogicalType;
import io.confluent.idesidecar.restapi.util.ByteArrayJsonUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;

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

  @ParameterizedTest(name = "shouldSerializeAndDeserializeAvroLogicalTypes: {1}")
  @MethodSource("avroLogicalTypeSchemas")
  default void shouldSerializeAndDeserializeAvroLogicalTypes(
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

  @Test
  @Disabled("There's something either wrong with the way the bytes "
      + "are sent or the way they are deserialized. "
      + "Left as an exercise for our future selves.")
  default void shouldSerializeAndDeserializeDecimalAvroLogicalType() throws IOException {
    var topicName = randomTopicName();
    createTopic(topicName);

    // A decimal logical type annotates Avro bytes or fixed types.
    // The byte array must contain the two's-complement representation of the unscaled
    // integer value in big-endian byte order.
    // The scale is fixed, and is specified using an attribute.
    var keySchema = """
        {
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 5,
          "scale": 2
        }
        """;

    var keySubjectName = getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, true);
    var createdKeySchema = createSchema(
        keySubjectName,
        SchemaFormat.AVRO.name(),
        keySchema
    );

    var valueSchema = loadResource("avro/record-with-decimal-logical-type.avsc");

    var valueSubjectName = getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, false);
    var createdValueSchema = createSchema(
        valueSubjectName,
        SchemaFormat.AVRO.name(),
        valueSchema
    );

    var recordBytes = RecordWithDecimalLogicalType
        .newBuilder()
        .setDecimalField(BigDecimal.valueOf(123456, 2))
        .build()
        .toByteBuffer()
        .array();

    var valueSchemaIdBytes = ByteBuffer
        .allocate(4)
        .putInt(createdValueSchema.getId() & 0xF)
        .array();

    var valueDataBytes = new byte[1 + recordBytes.length + valueSchemaIdBytes.length];
    valueDataBytes[0] = 0; // Magic byte
    System.arraycopy(valueSchemaIdBytes, 0, valueDataBytes, 1, valueSchemaIdBytes.length);
    System.arraycopy(recordBytes, 0, valueDataBytes, 1 + valueSchemaIdBytes.length, recordBytes.length);

    // Base64 encodes the bytes and places it in a JSON object,
    // {"__raw__": "base64-encoded-bytes"}
    var valueDataJson = ByteArrayJsonUtil.asJsonNode(valueDataBytes);

    // 12345 * 10^-2 = 123.45
    var sampleDecimalBytes = convertBigDecimalToBytes(12345, 2);
    var keySchemaIdBytes = ByteBuffer
        .allocate(4)
        .putInt(createdKeySchema.getId() & 0xF)
        .array();

    var keyDataBytes = new byte[1 + sampleDecimalBytes.length + keySchemaIdBytes.length];
    keyDataBytes[0] = 0; // Magic byte
    System.arraycopy(keySchemaIdBytes, 0, keyDataBytes, 1, keySchemaIdBytes.length);
    System.arraycopy(sampleDecimalBytes, 0, keyDataBytes, 1 + keySchemaIdBytes.length, sampleDecimalBytes.length);

    var keyDataJson = ByteArrayJsonUtil.asJsonNode(keyDataBytes);

    var key = RecordsV3BaseSuite.schemalessData("foo");
    produceRecord(
        topicName,
        keyDataJson,
        createdKeySchema.getVersion(),
        valueDataJson,
        createdValueSchema.getVersion()
    );

    assertTopicHasRecord(
        key,
        new RecordData(
            SchemaFormat.PROTOBUF,
            SubjectNameStrategyEnum.TOPIC_NAME,
            valueSchema,
            valueSubjectName,
            createdValueSchema.getId(),
            valueDataJson
        ),
        topicName
    );
  }

  static byte[] convertBigDecimalToBytes(long value, int scale) {
    return BigDecimal
        .valueOf(value, scale)
        .unscaledValue()
        .toByteArray();
  }
}
