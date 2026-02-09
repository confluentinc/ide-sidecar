package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

import static io.confluent.kafka.serializers.schema.id.SchemaId.KEY_SCHEMA_ID_HEADER;
import static io.confluent.kafka.serializers.schema.id.SchemaId.MAGIC_BYTE_V1;
import static io.confluent.kafka.serializers.schema.id.SchemaId.VALUE_SCHEMA_ID_HEADER;
import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryUtilTest {

    @Test
    void removeOAuthConfigs_shouldRemoveOnlyOAuthPrefixedKeys() {
      var input = new HashMap<String, Object>();
      input.put("bearer.auth.token.endpoint.url", "https://foo");
      input.put("bearer.auth.client.id", "bar");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertTrue(result.isEmpty());
    }

    @Test
    void removeOAuthConfigs_shouldLeaveNonOAuthKeysUntouched() {
      var input = new HashMap<String, Object>();
      input.put("schema.registry.url", "http://localhost:8081");
      input.put("basic.auth.user.info", "user:pass");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertEquals(input, result);
    }

    @Test
    void removeOAuthConfigs_shouldRemoveOnlyOAuthKeysAndLeaveOthersUntouched() {
      var input = new HashMap<String, Object>();
      input.put("bearer.auth.token.endpoint.url", "https://foo");
      input.put("schema.registry.url", "http://localhost:8081");
      input.put("bearer.auth.client.id", "bar");
      input.put("basic.auth.user.info", "user:pass");
      var expected = new HashMap<String, Object>();
      expected.put("schema.registry.url", "http://localhost:8081");
      expected.put("basic.auth.user.info", "user:pass");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertEquals(expected, result);
    }

    @Test
    void removeOAuthConfigs_shouldHandleEmptyMap() {
      var input = new HashMap<String, Object>();
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertTrue(result.isEmpty());
    }

    @Test
    void removeOAuthConfigs_shouldThrowNullPointerExceptionOnNullInput() {
      assertThrows(
          NullPointerException.class,
          () -> SchemaRegistryUtil.removeOAuthConfigs(null)
      );
    }

    @Test
    void decodeKafkaHeaders_shouldReturnEmptyListForEmptyHeaders() {
      var headers = new RecordHeaders();
      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);
      assertTrue(result.isEmpty());
    }

    @Test
    void toRecordHeaders_shouldConvertRegularHeadersToUtf8Strings() {
      var headers = new RecordHeaders();
      headers.add("content-type", "application/json".getBytes(StandardCharsets.UTF_8));
      headers.add("custom-header", "custom-value".getBytes(StandardCharsets.UTF_8));

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(2, result.size());
      assertEquals(new PartitionConsumeRecordHeader("content-type", "application/json"), result.get(0));
      assertEquals(new PartitionConsumeRecordHeader("custom-header", "custom-value"), result.get(1));
    }

    @Test
    void decodeKafkaHeaders_shouldHandleNullHeaderValue() {
      var headers = new RecordHeaders();
      headers.add("null-header", null);

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(new PartitionConsumeRecordHeader("null-header", null), result.get(0));
    }

    @Test
    void decodeKafkaHeaders_shouldDecodeKeySchemaIdHeaderAsUuid() {
      var uuid = UUID.randomUUID();
      var headers = new RecordHeaders();
      headers.add(KEY_SCHEMA_ID_HEADER, createSchemaGuidBytes(uuid));

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(KEY_SCHEMA_ID_HEADER, result.get(0).key());
      assertEquals(uuid.toString(), result.get(0).value());
    }

    @Test
    void decodeKafkaHeaders_shouldDecodeValueSchemaIdHeaderAsUuid() {
      var uuid = UUID.randomUUID();
      var headers = new RecordHeaders();
      headers.add(VALUE_SCHEMA_ID_HEADER, createSchemaGuidBytes(uuid));

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(VALUE_SCHEMA_ID_HEADER, result.get(0).key());
      assertEquals(uuid.toString(), result.get(0).value());
    }

    @Test
    void decodeKafkaHeaders_shouldReturnEmptyStringForInvalidKeySchemaIdHeader() {
      var headers = new RecordHeaders();
      // Too short value (less than 17 bytes)
      headers.add(KEY_SCHEMA_ID_HEADER, new byte[]{0x01, 0x02, 0x03});

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(KEY_SCHEMA_ID_HEADER, result.get(0).key());
      assertEquals("", result.get(0).value());
    }

    @Test
    void decodeKafkaHeaders_shouldReturnEmptyStringForInvalidValueSchemaIdHeader() {
      var headers = new RecordHeaders();
      // Too short value (less than 17 bytes)
      headers.add(VALUE_SCHEMA_ID_HEADER, new byte[]{0x01, 0x02, 0x03});

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(VALUE_SCHEMA_ID_HEADER, result.get(0).key());
      assertEquals("", result.get(0).value());
    }

    @Test
    void decodeKafkaHeaders_shouldReturnEmptyStringForWrongMagicByte() {
      var uuid = UUID.randomUUID();
      var headers = new RecordHeaders();
      var bytes = createSchemaGuidBytes(uuid);
      bytes[0] = MAGIC_BYTE_V1 + 1;

      headers.add(KEY_SCHEMA_ID_HEADER, bytes);

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(1, result.size());
      assertEquals(KEY_SCHEMA_ID_HEADER, result.get(0).key());
      assertEquals("", result.get(0).value());
    }

    @Test
    void decodeKafkaHeaders_shouldHandleMixedHeaders() {
      var keyUuid = UUID.randomUUID();
      var valueUuid = UUID.randomUUID();
      var headers = new RecordHeaders();
      headers.add("custom-header", "custom-value".getBytes(StandardCharsets.UTF_8));
      headers.add(KEY_SCHEMA_ID_HEADER, createSchemaGuidBytes(keyUuid));
      headers.add("another-header", "another-value".getBytes(StandardCharsets.UTF_8));
      headers.add(VALUE_SCHEMA_ID_HEADER, createSchemaGuidBytes(valueUuid));

      var result = SchemaRegistryUtil.decodeKafkaHeaders(headers);

      assertEquals(4, result.size());
      assertEquals(new PartitionConsumeRecordHeader("custom-header", "custom-value"), result.get(0));
      assertEquals(new PartitionConsumeRecordHeader(KEY_SCHEMA_ID_HEADER, keyUuid.toString()), result.get(1));
      assertEquals(new PartitionConsumeRecordHeader("another-header", "another-value"), result.get(2));
      assertEquals(new PartitionConsumeRecordHeader(VALUE_SCHEMA_ID_HEADER, valueUuid.toString()), result.get(3));
    }

    /**
     * Helper method to create schema GUID bytes in the expected format:
     * 1 byte magic byte (MAGIC_BYTE_V1) + 16 bytes UUID (8 bytes MSB + 8 bytes LSB)
     */
    private byte[] createSchemaGuidBytes(UUID uuid) {
      var buffer = ByteBuffer.allocate(17);
      buffer.put(MAGIC_BYTE_V1);
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      return buffer.array();
    }
}
