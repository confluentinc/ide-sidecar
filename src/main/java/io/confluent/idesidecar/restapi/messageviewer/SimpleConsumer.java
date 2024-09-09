package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.ExceededFields;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.TimestampType;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Implements consuming records from Confluent Local Kafka topics for the message viewer API.
 */
// CHECKSTYLE:OFF: ClassDataAbstractionCoupling
public class SimpleConsumer {
  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int MAX_POLLS = 5;
  private static final int MAX_POLL_RECORDS_LIMIT = 2_000;
  private static final int MAX_RESPONSE_BYTES = 20 * 1024 * 1024; // 20 MB
  private static final int DEFAULT_MESSAGE_MAX_BYTES = 4 * 1024 * 1024; // 4MB
  private static final int SR_CACHE_SIZE = 10;
  private SchemaRegistryClient schemaRegistryClient;

  final Properties baseConsumerConfig;

  public SimpleConsumer(Properties baseConfig) {
    var props = new Properties();
    // Custom properties
    props.putAll(baseConfig);

    // Create Schema Registry client
    String schemaRegistryUrl = (String) props.getOrDefault("schema.registry.url", null);
    if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
      this.schemaRegistryClient = new CachedSchemaRegistryClient(
          schemaRegistryUrl,
          SR_CACHE_SIZE,
          Arrays.asList(
              new ProtobufSchemaProvider(),
              new AvroSchemaProvider(),
              new JsonSchemaProvider()
          ),
          Collections.emptyMap()
      );
    }

    // Default properties
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    baseConsumerConfig = props;
  }

  @SuppressWarnings("CyclomaticComplexity")
  public List<PartitionConsumeData> consumeFromMultiplePartitions(
      String topicName,
      Map<Integer, Long> offsets,
      Boolean fromBeginning,
      Long timestamp,
      Integer maxPollRecords,
      Integer fetchMaxBytes,
      Integer messageMaxBytes
  ) {
    messageMaxBytes = messageMaxBytes == null ? DEFAULT_MESSAGE_MAX_BYTES : messageMaxBytes;
    // maxPollRecords is the number of messages we get for this entire query.
    int recordsLimit = Math.max(
        Optional.ofNullable(maxPollRecords).orElse(MAX_POLL_RECORDS_LIMIT),
        MAX_POLL_RECORDS_LIMIT
    );
    var consumer = getConsumerWithConfigs(recordsLimit, fetchMaxBytes);
    var numPartitions = getPartitionCount(consumer, topicName);
    var partitions = getPartitions(topicName, numPartitions, offsets);
    final var endOffsets = consumer.endOffsets(partitions);
    offsets = getInitialOffsets(
        consumer, partitions, offsets, endOffsets, fromBeginning, timestamp);

    var partitionRecordsMap = new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>();
    if (canConsumeFromOffsets(offsets, endOffsets)) {
      consumer.assign(partitions);
      for (TopicPartition partition : partitions) {
        consumer.seek(partition, offsets.get(partition.partition()));
      }
      var polls = 0;
      var responseSize = 0;
      while (partitionRecordsMap.size() < partitions.size()
             && polls++ < MAX_POLLS
             && responseSize <= MAX_RESPONSE_BYTES) {
        var partitionRecords = consumer.poll(POLL_TIMEOUT);
        for (var partition : partitions) {
          var kafkaRecords = partitionRecords.records(partition);
          if (kafkaRecords != null && !kafkaRecords.isEmpty()) {
            kafkaRecords = kafkaRecords.subList(
                0,
                Math.max(1, Math.min(kafkaRecords.size(), recordsLimit)));
            responseSize += getOverallSerializedMessageSize(kafkaRecords);
            // Depending on the set of while conditions above,
            // it's possible that the number of messages
            // can exceed the variable recordLimit.
            // The maximum number of messages that can be returned is
            // recordLimit + the number of partitions - 1
            recordsLimit -= kafkaRecords.size();
            partitionRecordsMap
                .computeIfAbsent(partition, p -> new ArrayList<>())
                .addAll(kafkaRecords);
          }
        }
        consumer.pause(partitionRecordsMap.keySet());
      }
    }

    var result = new ArrayList<PartitionConsumeData>();
    for (var partition : partitions) {
      var kafkaRecords = partitionRecordsMap.get(partition);
      result.add(
          getPartitionConsumeData(kafkaRecords, partition, offsets, endOffsets, messageMaxBytes)
      );
    }

    return result;
  }

  Integer getPartitionCount(final KafkaConsumer<byte[], byte[]> consumer, final String topicName) {
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
    if (partitionInfos == null) {
      return null;
    }
    return partitionInfos.size();
  }

  Map<Integer, Long> getInitialOffsets(
      final KafkaConsumer<byte[], byte[]> consumer,
      final List<TopicPartition> partitions,
      final Map<Integer, Long> offsets,
      final Map<TopicPartition, Long> endOffsets,
      final Boolean fromBeginning,
      final Long timestamp
  ) {
    Map<Integer, Long> initialOffsets;

    if (Boolean.TRUE.equals(fromBeginning)) {
      initialOffsets = consumer
          .beginningOffsets(partitions)
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().partition(),
                  Entry::getValue
              )
          );
    } else if (Boolean.FALSE.equals(fromBeginning)) {
      initialOffsets = endOffsets
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().partition(),
                  Entry::getValue
              )
          );
    } else if (timestamp != null) {
      Map<TopicPartition, Long> timestampMap = partitions
          .stream()
          .collect(
              Collectors.toMap(
                  tp -> tp,
                  tp -> timestamp
              )
          );
      Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampMap);

      initialOffsets = offsetMap
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().partition(),
                  entry -> {
                    if (entry.getValue() != null) {
                      return entry.getValue().offset();
                    } else {
                      return endOffsets.get(entry.getKey());
                    }
                  }));
    } else if (offsets != null) {
      // Position supplied as a map of TopicPartition->offsets.
      // Guard against seeking to an offset earlier than the log start offset.
      initialOffsets = consumer
          .beginningOffsets(partitions)
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  entry -> entry.getKey().partition(),
                  entry -> Long.max(entry.getValue(), offsets.get(entry.getKey().partition()))
              )
          );
    } else {
      // Any other case, set to the endOffsets
      initialOffsets = endOffsets
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                entry -> entry.getKey().partition(),
                Entry::getValue
              )
          );
    }

    return initialOffsets;
  }

  KafkaConsumer<byte[], byte[]> getConsumerWithConfigs(
      Integer maxPollRecords,
      Integer fetchMaxBytes
  ) {
    var consumerConfigOverride = baseConsumerConfig;
    if (maxPollRecords != null) {
      consumerConfigOverride.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    }
    if (fetchMaxBytes != null) {
      consumerConfigOverride.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
    }
    return new KafkaConsumer<>(consumerConfigOverride);
  }

  List<TopicPartition> getPartitions(
      String topicName,
      int numPartitions,
      Map<Integer, Long> offsets
  ) {
    if (offsets == null || offsets.isEmpty()) {
      return IntStream
          .range(0, numPartitions)
          .mapToObj(partitionId -> new TopicPartition(topicName, partitionId))
          .collect(Collectors.toList());
    }
    return offsets
        .keySet()
        .stream()
        .map(partitionId -> new TopicPartition(topicName, partitionId))
        .collect(Collectors.toList());
  }

  boolean canConsumeFromOffsets(Map<Integer, Long> offsets, Map<TopicPartition, Long> endOffsets) {
    if (offsets == null || offsets.isEmpty()) {
      return false;
    }
    for (var endOffset : endOffsets.entrySet()) {
      var partitionEndOffset = endOffset.getValue();
      var offset = offsets.get(endOffset.getKey().partition());
      if (partitionEndOffset >= offset) {
        return true;
      }
    }
    return false;
  }

  int getOverallSerializedMessageSize(List<ConsumerRecord<byte[], byte[]>> kafkaRecords) {
    var result = 0;
    for (var rec : kafkaRecords) {
      result += rec.serializedKeySize() + rec.serializedValueSize();
    }
    return result;
  }

  PartitionConsumeData getPartitionConsumeData(
      List<ConsumerRecord<byte[], byte[]>> singlePartitionRecords,
      TopicPartition partition,
      Map<Integer, Long> offsets,
      Map<TopicPartition, Long> endOffsets,
      Integer messageMaxBytes
  ) {
    final var partitionId = partition.partition();
    // Initialize the next offset with the currently known end offset for the partition.
    var nextOffset = endOffsets.get(partition);
    if (offsets != null && offsets.containsKey(partitionId)) {
      // Set the next offset to the provided offset (if there is one) for the partition.
      nextOffset = Math.min(nextOffset, offsets.get(partitionId));
    }
    var numRecords = singlePartitionRecords != null ? singlePartitionRecords.size() : 0;
    if (numRecords > 0) {
      // Set the next offset to 1 plus the offset of the last consumed record (if there is one).
      nextOffset = singlePartitionRecords.get(numRecords - 1).offset() + 1;
    }

    return new PartitionConsumeData(
        partitionId,
        nextOffset,
        listFromConsumerRecordList(singlePartitionRecords, messageMaxBytes)
    );
  }

  List<PartitionConsumeRecord> listFromConsumerRecordList(
      List<ConsumerRecord<byte[], byte[]>> consumerRecords,
      Integer messageMaxBytes
  ) {
    return consumerRecords == null
        ? Collections.emptyList()
        : consumerRecords.stream()
            .map(consumerRecord -> mapConsumerRecord(consumerRecord, messageMaxBytes))
            .toList();
  }

  PartitionConsumeRecord mapConsumerRecord(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      Integer messageMaxBytes) {
    var headers = new ArrayList<PartitionConsumeRecordHeader>();
    for (var header : consumerRecord.headers()) {
      headers.add(
          new PartitionConsumeRecordHeader(
              header.key(),
              new String(header.value(), StandardCharsets.UTF_8)
          )
      );
    }

    // Determine if the key or value exceeds the maximum allowed size
    boolean keyExceeded = consumerRecord.key() != null
        && consumerRecord.key().length > messageMaxBytes;
    boolean valueExceeded = consumerRecord.value() != null
        && consumerRecord.value().length > messageMaxBytes;

    var keyResult = keyExceeded ? null : DecoderUtil.parseJsonNode(
        consumerRecord.key(),
        schemaRegistryClient,
        consumerRecord.topic());
    var valueResult = valueExceeded ? null : DecoderUtil.parseJsonNode(
        consumerRecord.value(),
        schemaRegistryClient,
        consumerRecord.topic());

    return new PartitionConsumeRecord(
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        TimestampType.valueOf(consumerRecord.timestampType().name()),
        headers,
        keyResult == null ? null : keyResult.getValue(),
        valueResult == null ? null : valueResult.getValue(),
        keyResult == null ? null : keyResult.getErrorMessage(),
        valueResult == null ? null : valueResult.getErrorMessage(),
        new ExceededFields(keyExceeded, valueExceeded)
    );
  }
}
// CHECKSTYLE:ON: ClassDataAbstractionCoupling
