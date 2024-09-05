package io.confluent.idesidecar.restapi.messageviewer.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;

/**
 * Represents the data consumed from multiple partitions in a Kafka topic.
 *
 * @param clusterId The ID of the Kafka cluster.
 * @param topicName The name of the Kafka topic.
 * @param partitionDataList The list of partition data consumed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@RegisterForReflection
public record SimpleConsumeMultiPartitionResponse(
    @JsonProperty("cluster_id") String clusterId,
    @JsonProperty("topic_name") String topicName,
    @JsonProperty("partition_data_list") List<PartitionConsumeData> partitionDataList
) {
  /**
   * Represents the data consumed from a single partition.
   *
   * @param partitionId The ID of the partition.
   * @param nextOffset The next offset to consume from.
   * @param records The list of records consumed from this partition.
   */
  @RegisterForReflection
  public record PartitionConsumeData(
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("next_offset") long nextOffset,
      @JsonProperty("records") List<PartitionConsumeRecord> records
  ) {}

  @RegisterForReflection
  public record ExceededFields(
      @JsonProperty("key") boolean key,
      @JsonProperty("value") boolean value
  ) {}

  /**
   * Represents a single record consumed from a partition.
   *
   * @param partitionId The ID of the partition.
   * @param offset The offset of the record.
   * @param timestamp The timestamp of the record.
   * @param timestampType The type of the timestamp (e.g., CREATE_TIME).
   * @param headers The list of headers associated with the record.
   * @param key The key of the record, decoded if applicable.
   * @param value The value of the record, decoded if applicable.
   * @param keyDecodingError A string containing an error message if key decoding failed;
   *                         null if decoding was successful or not attempted.
   * @param valueDecodingError A string containing an error message if value decoding failed;
   *                           null if decoding was successful or not attempted.
   */
  @RegisterForReflection
  public record PartitionConsumeRecord(
      @JsonProperty("partition_id") int partitionId,
      @JsonProperty("offset") long offset,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("timestamp_type") TimestampType timestampType,
      @JsonProperty("headers") List<PartitionConsumeRecordHeader> headers,
      @JsonProperty("key") JsonNode key,
      @JsonProperty("value") JsonNode value,
      @JsonProperty("key_decoding_error") String keyDecodingError,
      @JsonProperty("value_decoding_error") String valueDecodingError,
      @JsonProperty("exceeded_fields") ExceededFields exceededFields
  ) {
    // Initialize key and value decoding errors to null by default
    public PartitionConsumeRecord(
        int partitionId,
        long offset,
        long timestamp,
        TimestampType timestampType,
        List<PartitionConsumeRecordHeader> headers,
        JsonNode key,
        JsonNode value,
        ExceededFields exceededFields
    ) {
      this(
          partitionId,
          offset,
          timestamp,
          timestampType,
          headers, key, value,
          null,
          null,
          exceededFields == null ? new ExceededFields(false, false) : exceededFields
      );
    }
  }

  /**
   * Represents a header of a record.
   *
   * @param key The key of the header.
   * @param value The value of the header.
   */
  @RegisterForReflection
  public record PartitionConsumeRecordHeader(
      @JsonProperty("key") String key,
      @JsonProperty("value") String value
  ) {}


  /**
   * Represents a value encoded as a Base64 string.
   *
   * @param raw The Base64 encoded string.
   */
  @RegisterForReflection
  public record SchemaEncodedValue(
      @JsonProperty("__raw__") String raw // Base64 encoded string
  ) {}

  /**
   * @see org.apache.kafka.common.record.TimestampType
   */
  @RegisterForReflection
  public enum TimestampType {
    NO_TIMESTAMP_TYPE,
    CREATE_TIME,
    LOG_APPEND_TIME;
  }
}
