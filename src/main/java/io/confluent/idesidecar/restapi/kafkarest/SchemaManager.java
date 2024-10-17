package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
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

  public ParsedSchema parseSchema(Schema schema) {
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
    AVRO {
      private final SchemaProvider schemaProvider = new AvroSchemaProvider();

      @Override
      SchemaProvider schemaProvider() {
        return schemaProvider;
      }
    },
    PROTOBUF {
      private final SchemaProvider schemaProvider = new ProtobufSchemaProvider();

      @Override
      SchemaProvider schemaProvider() {
        return schemaProvider;
      }
    },
    JSON {
      private final SchemaProvider schemaProvider = new JsonSchemaProvider();

      @Override
      SchemaProvider schemaProvider() {
        return schemaProvider;
      }
    };

    abstract SchemaProvider schemaProvider();

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
}
