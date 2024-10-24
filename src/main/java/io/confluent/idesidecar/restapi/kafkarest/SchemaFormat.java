package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.Arrays;

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
  public static SchemaFormat fromSchemaType(String schemaType) {
    return Arrays.stream(values())
        .filter(format -> format.schemaProvider() != null)
        .filter(format -> format.name().equalsIgnoreCase(schemaType))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Illegal schema type: " + schemaType));
  }
}
