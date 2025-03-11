package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer.DecodedResult;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.ExceededFields;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.RecordMetadata;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.TimestampType;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Implements consuming records from Kafka topics for the message viewer API.
 */
public class SimpleConsumer {

  private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
  private static final int MAX_POLLS = 5;
  private static final int MAX_POLL_RECORDS_LIMIT = 2_000;
  private static final int MAX_RESPONSE_BYTES = 20 * 1024 * 1024; // 20 MB
  private static final int DEFAULT_MESSAGE_MAX_BYTES = 4 * 1024 * 1024; // 4MB
  private final SchemaRegistryClient schemaRegistryClient;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final RecordDeserializer recordDeserializer;
  private final KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context;

  public SimpleConsumer(
      KafkaConsumer<byte[], byte[]> consumer,
      SchemaRegistryClient sr,
      RecordDeserializer recordDeserializer,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context
  ) {
    this.consumer = consumer;
    this.schemaRegistryClient = sr;
    this.recordDeserializer = recordDeserializer;
    this.context = context;
  }

  @SuppressWarnings("CyclomaticComplexity")
  public List<PartitionConsumeData> consume(
      String topicName,
      SimpleConsumeMultiPartitionRequest request) {
    final var messageMaxBytes = Optional
        .ofNullable(request.messageMaxBytes())
        .orElse(DEFAULT_MESSAGE_MAX_BYTES);

    // maxPollRecords is the number of messages we get for this entire query.
    var recordsLimit = Math.min(
        Optional.ofNullable(request.maxPollRecords()).orElse(MAX_POLL_RECORDS_LIMIT),
        MAX_POLL_RECORDS_LIMIT
    );
    final var numPartitions = getPartitionCount(consumer, topicName);
    final var partitions = getPartitions(topicName, numPartitions, request.offsetsByPartition());
    final var endOffsets = consumer.endOffsets(partitions);
    final var offsets = getInitialOffsets(
        consumer,
        partitions,
        request.offsetsByPartition(),
        endOffsets,
        request.fromBeginning(),
        request.timestamp()
    );

    final var partitionRecordsMap =
        new HashMap<TopicPartition, List<ConsumerRecord<byte[], byte[]>>>();
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
    var headers = getPartitionConsumeRecordHeaders(consumerRecord);

    // Determine if the key or value exceeds the maximum allowed size
    boolean keyExceeded = consumerRecord.key() != null
        && consumerRecord.key().length > messageMaxBytes;
    boolean valueExceeded = consumerRecord.value() != null
        && consumerRecord.value().length > messageMaxBytes;

    Optional<DecodedResult> keyResult = keyExceeded
        ? Optional.empty() : Optional
        .of(recordDeserializer.deserialize(
            consumerRecord.key(),
            schemaRegistryClient,
            context,
            true)
        );
    Optional<DecodedResult> valueResult = valueExceeded
        ? Optional.empty() : Optional
        .of(recordDeserializer.deserialize(
            consumerRecord.value(),
            schemaRegistryClient,
            context,
            false)
        );

    return new PartitionConsumeRecord(
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        TimestampType.valueOf(consumerRecord.timestampType().name()),
        headers,
        keyResult.map(DecodedResult::value).orElse(null),
        valueResult.map(DecodedResult::value).orElse(null),
        new RecordMetadata(
            keyResult.map(DecodedResult::metadata).orElse(null),
            valueResult.map(DecodedResult::metadata).orElse(null)
        ),
        keyResult.map(DecodedResult::errorMessage).orElse(null),
        valueResult.map(DecodedResult::errorMessage).orElse(null),
        new ExceededFields(keyExceeded, valueExceeded)
    );
  }

  private static ArrayList<PartitionConsumeRecordHeader> getPartitionConsumeRecordHeaders(
      ConsumerRecord<byte[], byte[]> consumerRecord
  ) {
    var headers = new ArrayList<PartitionConsumeRecordHeader>();
    for (var header : consumerRecord.headers()) {
      headers.add(
          new PartitionConsumeRecordHeader(
              header.key(),
              new String(header.value(), StandardCharsets.UTF_8)
          )
      );
    }
    return headers;
  }
}
// CHECKSTYLE:ON: ClassDataAbstractionCoupling
