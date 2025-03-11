package io.confluent.idesidecar.restapi.models;

import io.confluent.idesidecar.restapi.kafkarest.SchemaFormat;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description = """
        The data format that represents the bytes of a Kafka message.
        
        * AVRO: Apache Avro schema format.
        * JSONSCHEMA: JSON schema format.
        * PROTOBUF: Google Protocol Buffers schema format.
        * JSON:
            Bytes parsed as JSON.
            The bytes did not contain a magic byte specifying a schema id, and the raw bytes
            were successfully parsed into a JSON value.
        * UTF8_STRING:
            Bytes parsed as a UTF-8 string (meaning the bytes were not parsed as valid JSON).
        * RAW_BYTES:
            Raw bytes. These are the scenarios where it would be used:
            - Arbitrary bytes that are NOT written/read using an implementation of Kafka serializer/deserializer.
            And further, we tried to but could not interpret these bytes as a JSON value.
            - The Kafka serializer/deserializer known to sidecar failed to parse the schematized bytes
            (meaning the schema id was present in the magic bytes, but our classes could not interpret the rest of the bytes.)
        """
)
public enum DataFormat {
  AVRO,
  JSONSCHEMA,
  PROTOBUF,
  JSON,
  UTF8_STRING,
  RAW_BYTES;

  /**
   * Get the {@link DataFormat} for the given schema format.
   */
  public static DataFormat fromSchemaFormat(SchemaFormat format) {
    return switch (format) {
      case SchemaFormat.AVRO -> AVRO;
      case SchemaFormat.JSON -> JSONSCHEMA;
      case SchemaFormat.PROTOBUF -> PROTOBUF;
    };
  }

  public boolean isSchemaless() {
    return this == RAW_BYTES || this == JSON || this == UTF8_STRING;
  }
}
