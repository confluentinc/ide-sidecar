package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import static io.confluent.idesidecar.restapi.kafkarest.RecordSerializer.wrappedToObject;

/**
 * Encapsulates logic to serialize Avro data. Has special handling for Avro logical types.
 */
public final class AvroRecordSerializer {

  private AvroRecordSerializer() {
  }

  static GenericData GENERIC_DATA = new GenericData();

  static {
    addLogicalTypeConversion(GENERIC_DATA);
  }

  public static ByteString serialize(
      SchemaRegistryClient client,
      ParsedSchema parsedSchema,
      Map<String, String> configs,
      String topicName,
      JsonNode data,
      boolean isKey
  ) {
    try (var avroSerializer = new KafkaAvroSerializer(client)) {
      configs.put(
          KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, "true"
      );
      avroSerializer.configure(configs, isKey);
      var schema = (AvroSchema) parsedSchema;
      Object record;
      if (isLogicalTypeSchema(schema.rawSchema())) {
        record = wrappedToObject(() -> AvroSchemaUtils.toObject(
            data, schema, new GenericDatumReader<>(
                schema.rawSchema(), schema.rawSchema(), GENERIC_DATA
            )
        ));
      } else {
        record = wrappedToObject(() -> AvroSchemaUtils.toObject(data, schema));
      }
      return ByteString.copyFrom(avroSerializer.serialize(topicName, record));
    }
  }

  private static boolean isLogicalTypeSchema(Schema schema) {
    return schema.getLogicalType() != null;
  }

  /**
   * Converts logical types to a {@link NonRecordContainer} type. We wrap the existing
   * {@link Conversion} implementations into a new {@link NonRecordContainerConversion} so that
   * the {@link AvroSchemaUtils#getSchema} plays nicely with logical types.
   */
  public static void addLogicalTypeConversion(GenericData avroData) {
    // Reference: https://github.com/confluentinc/schema-registry/blob/5e3310e918e358a831555df0052d2db01771708b/client/src/main/java/io/confluent/kafka/schemaregistry/avro/AvroSchemaUtils.java#L168-L182
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new Conversions.DecimalConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new Conversions.UUIDConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.DateConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.TimeMillisConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.TimeMicrosConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.TimestampMillisConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.TimestampMicrosConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.LocalTimestampMillisConversion())
    );
    avroData.addLogicalTypeConversion(
        new NonRecordContainerConversion<>(new TimeConversions.LocalTimestampMicrosConversion())
    );
  }

  /**
   * A {@link Conversion} implementation that wraps another {@link Conversion} implementation.
   * The methods to convert into a {@link NonRecordContainer} type are NOT delegated to the parent,
   * but instead the value passed is used as-is, see {@link #fromLong} for example. Whereas, the
   * methods to convert from a {@link NonRecordContainer} type are delegated to the parent, see
   * {@link #toLong} for example.
   * @param <U>
   * @param <T>
   */
  static class NonRecordContainerConversion<U, T extends Conversion<U>>
          extends Conversion<NonRecordContainer> {
    final T parent;

    public NonRecordContainerConversion(T parent) {
      this.parent = parent;
    }

    @Override
    public Class<NonRecordContainer> getConvertedType() {
      return NonRecordContainer.class;
    }

    @Override
    public String getLogicalTypeName() {
      return parent.getLogicalTypeName();
    }

    public String adjustAndSetValue(String varName, String valParamName) {
      return parent.adjustAndSetValue(varName, valParamName);
    }

    public Schema getRecommendedSchema() {
      return parent.getRecommendedSchema();
    }

    public NonRecordContainer fromBoolean(Boolean value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromInt(Integer value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromLong(Long value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromFloat(Float value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromDouble(Double value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromEnumSymbol(GenericEnumSymbol<?> value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromFixed(GenericFixed value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromArray(Collection<?> value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public NonRecordContainer fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
      return new NonRecordContainer(schema, value);
    }

    public Boolean toBoolean(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toBoolean((U) value.getValue(), schema, type);
    }

    public Integer toInt(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toInt((U) value.getValue(), schema, type);
    }

    public Long toLong(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toLong((U) value.getValue(), schema, type);
    }

    public Float toFloat(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toFloat((U) value.getValue(), schema, type);
    }

    public Double toDouble(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toDouble((U) value.getValue(), schema, type);
    }

    public CharSequence toCharSequence(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toCharSequence((U) value.getValue(), schema, type);
    }

    public GenericEnumSymbol<?> toEnumSymbol(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toEnumSymbol((U) value.getValue(), schema, type);
    }

    public GenericFixed toFixed(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toFixed((U) value.getValue(), schema, type);
    }

    public ByteBuffer toBytes(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toBytes((U) value.getValue(), schema, type);
    }

    public Collection<?> toArray(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toArray((U) value.getValue(), schema, type);
    }

    public Map<?, ?> toMap(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toMap((U) value.getValue(), schema, type);
    }

    public IndexedRecord toRecord(NonRecordContainer value, Schema schema, LogicalType type) {
      return parent.toRecord((U) value.getValue(), schema, type);
    }
  }
}
