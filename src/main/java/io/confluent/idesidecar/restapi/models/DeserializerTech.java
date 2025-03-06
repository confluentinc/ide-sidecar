package io.confluent.idesidecar.restapi.models;

import io.confluent.idesidecar.restapi.kafkarest.SchemaFormat;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public enum DeserializerTech {
  // Schema formats
  AVRO,
  JSONSCHEMA,
  PROTOBUF,

  @Schema(description = """
      Bytes parsed as JSON.
      The bytes did not contain a magic byte specifying a schema id, and the raw bytes were successfully
      parsed into a JSON value (string, number, object, array, true, false, null).
      """)
  PARSED_JSON,

  @Schema(description = """
   Raw bytes. These are the scenarios where it would be used:
   <ul>
     <li>Arbitrary bytes that are NOT written/read using an implementation of Kafka serializer/deserializer.
     And further, we tried to but could not interpret these bytes as a JSON value (string, number, object, array, true, false, null)
     </li>
     <li>The Kafka serializer/deserializer known to sidecar failed to parse the schematized bytes
     (meaning the schema id was present in the magic bytes, but our classes could not interpret the rest of the bytes.)
     </li>
   </ul>
   """)
  RAW;

  /**
   * Get the DeserializerTech for the given schema format.
   */
  public static DeserializerTech fromSchemaFormat(SchemaFormat format) {
    return switch (format) {
      case SchemaFormat.AVRO -> AVRO;
      case SchemaFormat.JSON -> JSONSCHEMA;
      case SchemaFormat.PROTOBUF -> PROTOBUF;
    };
  }

  public boolean isSchemaless() {
    return this == RAW || this == PARSED_JSON;
  }
}
