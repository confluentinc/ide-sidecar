package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader;
import org.apache.kafka.common.header.Headers;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.confluent.kafka.serializers.schema.id.SchemaId.KEY_SCHEMA_ID_HEADER;
import static io.confluent.kafka.serializers.schema.id.SchemaId.VALUE_SCHEMA_ID_HEADER;

public final class SchemaRegistryUtil {

  private SchemaRegistryUtil() {
    // Utility class, no instantiation needed
  }

  /**
   * Remove OAuth-related configuration properties from the given Schema Registry configuration map.
   *
   * @param configProperties The original Schema Registry configuration properties.
   * @return A new map with OAuth configuration properties removed.
   */
  public static Map<String, Object> removeOAuthConfigs(Map<String, Object> configProperties) {
    var filteredConfig = new HashMap<String, Object>();
    for (var configProperty : configProperties.entrySet()) {
      String key = configProperty.getKey();
      if (!key.startsWith("bearer.auth.")) {
        filteredConfig.put(key, configProperty.getValue());
      }
    }
    return filteredConfig;
  }

  /**
   * Converts Kafka Headers to a list of PartitionConsumeRecordHeader with decoded string values.
   *
   * <p>Header values are decoded as follows:
   * <ul>
   *   <li>For schema ID headers ({@code confluent.key.schemaId} and {@code confluent.value.schemaId}):
   *       the value is decoded as a schema GUID and converted to a UUID string. If the GUID cannot
   *       be extracted (invalid format or insufficient bytes), an empty string is returned.</li>
   *   <li>For other headers with non-null values: the byte array is decoded as a UTF-8 string.</li>
   *   <li>For headers with null values: the decoded value is null.</li>
   * </ul>
   *
   * @param headers the Kafka Headers to convert
   * @return a list of PartitionConsumeRecordHeader with decoded string values
   */
  public static List<PartitionConsumeRecordHeader> decodeKafkaHeaders(Headers headers) {
    var result = new ArrayList<PartitionConsumeRecordHeader>();
    for (var header : headers) {
      String decodedValue = null;
      if (KEY_SCHEMA_ID_HEADER.equals(header.key())) {
        decodedValue = RecordDeserializer
            .getSchemaGuidFromHeaders(headers, true)
            .map(UUID::toString)
            .orElse("");
      } else if (VALUE_SCHEMA_ID_HEADER.equals(header.key())) {
        decodedValue = RecordDeserializer
            .getSchemaGuidFromHeaders(headers, false)
            .map(UUID::toString)
            .orElse("");
      } else if (header.value() != null) {
        decodedValue = new String(header.value(), StandardCharsets.UTF_8);
      }
      result.add(
          new PartitionConsumeRecordHeader(
              header.key(),
              decodedValue
          )
      );
    }
    return result;
  }
}
