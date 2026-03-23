package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumer;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerAssignment;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerAssignments;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerGroup;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerGroupLagSummary;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerGroups;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerLag;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumerLags;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forConsumers;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forController;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartition;
import io.confluent.idesidecar.restapi.kafkarest.api.ConsumerGroupV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerAssignmentData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerAssignmentDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerGroupData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerGroupDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerGroupLagSummaryData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerLagData;
import io.confluent.idesidecar.restapi.kafkarest.model.ConsumerLagDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@RequestScoped
public class ConsumerGroupV3ApiImpl implements ConsumerGroupV3Api {

  @Inject
  ConsumerGroupManager consumerGroupManager;

  @Override
  public Uni<ConsumerGroupDataList> listKafkaConsumerGroups(String clusterId) {
    return consumerGroupManager
        .listKafkaConsumerGroups(clusterId)
        .map(descriptions -> ConsumerGroupDataList.builder()
            .kind("KafkaConsumerGroupList")
            .metadata(ResourceCollectionMetadata.builder()
                .next(null)
                .self(forConsumerGroups(clusterId).getRelated())
                .build())
            .data(descriptions.stream()
                .map(desc -> toConsumerGroupData(clusterId, desc))
                .toList())
            .build());
  }

  @Override
  public Uni<ConsumerGroupData> getKafkaConsumerGroup(
      String clusterId, String consumerGroupId
  ) {
    return consumerGroupManager
        .getKafkaConsumerGroup(clusterId, consumerGroupId)
        .map(desc -> toConsumerGroupData(clusterId, desc));
  }

  @Override
  public Uni<ConsumerDataList> listKafkaConsumers(
      String clusterId, String consumerGroupId
  ) {
    return consumerGroupManager
        .getKafkaConsumerGroup(clusterId, consumerGroupId)
        .map(desc -> ConsumerDataList.builder()
            .kind("KafkaConsumerList")
            .metadata(ResourceCollectionMetadata.builder()
                .next(null)
                .self(forConsumers(clusterId, consumerGroupId).getRelated())
                .build())
            .data(desc.members().stream()
                .map(member -> toConsumerData(clusterId, consumerGroupId, member))
                .toList())
            .build());
  }

  @Override
  public Uni<ConsumerData> getKafkaConsumer(
      String clusterId, String consumerGroupId, String consumerId
  ) {
    return consumerGroupManager
        .getKafkaConsumerGroup(clusterId, consumerGroupId)
        .map(desc -> desc.members().stream()
            .filter(m -> m.consumerId().equals(consumerId))
            .findFirst()
            .map(member -> toConsumerData(clusterId, consumerGroupId, member))
            .orElseThrow(() -> new NotFoundException(
                "Consumer '%s' not found in consumer group '%s'."
                    .formatted(consumerId, consumerGroupId))));
  }

  @Override
  public Uni<ConsumerGroupLagSummaryData> getKafkaConsumerGroupLagSummary(
      String clusterId, String consumerGroupId
  ) {
    return buildLagData(clusterId, consumerGroupId)
        .map(lagEntries -> {
          long maxLag = 0;
          long totalLag = 0;
          ConsumerLagData maxLagEntry = null;

          for (var entry : lagEntries) {
            totalLag += entry.getLag();
            if (maxLagEntry == null || entry.getLag() > maxLag) {
              maxLag = entry.getLag();
              maxLagEntry = entry;
            }
          }

          // handle empty lag list (group has no committed offsets)
          if (maxLagEntry == null) {
            return ConsumerGroupLagSummaryData.builder()
                .kind("KafkaConsumerGroupLagSummary")
                .metadata(ResourceMetadata.builder()
                    .resourceName(null)
                    .self(forConsumerGroupLagSummary(
                        clusterId, consumerGroupId).getRelated())
                    .build())
                .clusterId(clusterId)
                .consumerGroupId(consumerGroupId)
                .maxLagConsumerId("")
                .maxLagClientId("")
                .maxLagTopicName("")
                .maxLagPartitionId(0)
                .maxLag(0L)
                .totalLag(0L)
                .maxLagConsumer(forConsumer(clusterId, consumerGroupId, ""))
                .maxLagPartition(forPartition(clusterId, "", 0))
                .build();
          }

          return ConsumerGroupLagSummaryData.builder()
              .kind("KafkaConsumerGroupLagSummary")
              .metadata(ResourceMetadata.builder()
                  .resourceName(null)
                  .self(forConsumerGroupLagSummary(
                      clusterId, consumerGroupId).getRelated())
                  .build())
              .clusterId(clusterId)
              .consumerGroupId(consumerGroupId)
              .maxLagConsumerId(maxLagEntry.getConsumerId())
              .maxLagInstanceId(maxLagEntry.getInstanceId())
              .maxLagClientId(maxLagEntry.getClientId())
              .maxLagTopicName(maxLagEntry.getTopicName())
              .maxLagPartitionId(maxLagEntry.getPartitionId())
              .maxLag(maxLag)
              .totalLag(totalLag)
              .maxLagConsumer(forConsumer(
                  clusterId, consumerGroupId, maxLagEntry.getConsumerId()))
              .maxLagPartition(forPartition(
                  clusterId, maxLagEntry.getTopicName(), maxLagEntry.getPartitionId()))
              .build();
        });
  }

  @Override
  public Uni<ConsumerLagDataList> listKafkaConsumerLags(
      String clusterId, String consumerGroupId
  ) {
    return buildLagData(clusterId, consumerGroupId)
        .map(lagEntries -> ConsumerLagDataList.builder()
            .kind("KafkaConsumerLagList")
            .metadata(ResourceCollectionMetadata.builder()
                .next(null)
                .self(forConsumerLags(clusterId, consumerGroupId).getRelated())
                .build())
            .data(lagEntries)
            .build());
  }

  @Override
  public Uni<ConsumerLagData> getKafkaConsumerLag(
      String clusterId, String consumerGroupId, String topicName, Integer partitionId
  ) {
    return buildLagData(clusterId, consumerGroupId)
        .map(lagEntries -> lagEntries.stream()
            .filter(lag -> lag.getTopicName().equals(topicName)
                && lag.getPartitionId().equals(partitionId))
            .findFirst()
            .orElseThrow(() -> new NotFoundException(
                "Consumer lag for topic '%s' partition %d not found in consumer group '%s'."
                    .formatted(topicName, partitionId, consumerGroupId))));
  }

  @Override
  public Uni<ConsumerAssignmentDataList> listKafkaConsumerAssignment(
      String clusterId, String consumerGroupId, String consumerId
  ) {
    return consumerGroupManager
        .getKafkaConsumerGroup(clusterId, consumerGroupId)
        .map(desc -> {
          var member = desc.members().stream()
              .filter(m -> m.consumerId().equals(consumerId))
              .findFirst()
              .orElseThrow(() -> new NotFoundException(
                  "Consumer '%s' not found in consumer group '%s'."
                      .formatted(consumerId, consumerGroupId)));
          return ConsumerAssignmentDataList.builder()
              .kind("KafkaConsumerAssignmentList")
              .metadata(ResourceCollectionMetadata.builder()
                  .next(null)
                  .self(forConsumerAssignments(
                      clusterId, consumerGroupId, consumerId).getRelated())
                  .build())
              .data(member.assignment().topicPartitions().stream()
                  .map(tp -> toConsumerAssignmentData(
                      clusterId, consumerGroupId, consumerId, tp))
                  .toList())
              .build();
        });
  }

  @Override
  public Uni<ConsumerAssignmentData> getKafkaConsumerAssignment(
      String clusterId, String consumerGroupId, String consumerId,
      String topicName, Integer partitionId
  ) {
    return listKafkaConsumerAssignment(clusterId, consumerGroupId, consumerId)
        .map(list -> list.getData().stream()
            .filter(a -> a.getTopicName().equals(topicName)
                && a.getPartitionId().equals(partitionId))
            .findFirst()
            .orElseThrow(() -> new NotFoundException(
                "Assignment for topic '%s' partition %d not found for consumer '%s'."
                    .formatted(topicName, partitionId, consumerId))));
  }

  /**
   * Builds per-partition lag data by combining committed offsets with log end offsets
   * and correlating with consumer group member assignments.
   */
  private Uni<List<ConsumerLagData>> buildLagData(
      String clusterId, String consumerGroupId
  ) {
    // first get the consumer group description and committed offsets in parallel
    return Uni.combine().all()
        .unis(
            consumerGroupManager.getKafkaConsumerGroup(clusterId, consumerGroupId),
            consumerGroupManager.listConsumerGroupOffsets(clusterId, consumerGroupId))
        .asTuple()
        .chain(tuple -> {
          var description = tuple.getItem1();
          var committedOffsets = tuple.getItem2();

          // then fetch log end offsets for all partitions that have committed offsets
          return consumerGroupManager
              .getLogEndOffsets(clusterId, committedOffsets.keySet())
              .map(logEndOffsets ->
                  toLagDataList(
                      clusterId, consumerGroupId,
                      description, committedOffsets, logEndOffsets));
        });
  }

  private List<ConsumerLagData> toLagDataList(
      String clusterId,
      String consumerGroupId,
      ConsumerGroupDescription description,
      Map<TopicPartition, OffsetAndMetadata> committedOffsets,
      Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>
          logEndOffsets
  ) {
    // build a mapping from TopicPartition -> member for consumer attribution
    var tpToMember = new HashMap<TopicPartition, MemberDescription>();
    for (var member : description.members()) {
      for (var tp : member.assignment().topicPartitions()) {
        tpToMember.put(tp, member);
      }
    }

    var lagEntries = new ArrayList<ConsumerLagData>();
    for (var entry : committedOffsets.entrySet()) {
      var tp = entry.getKey();
      var committed = entry.getValue();
      var logEnd = logEndOffsets.get(tp);
      long currentOffset = committed.offset();
      long endOffset = logEnd != null ? logEnd.offset() : currentOffset;
      long lag = Math.max(0, endOffset - currentOffset);

      var member = tpToMember.get(tp);
      String consumerId = member != null ? member.consumerId() : "";
      String instanceId = member != null
          ? member.groupInstanceId().orElse(null) : null;
      String clientId = member != null ? member.clientId() : "";

      lagEntries.add(ConsumerLagData.builder()
          .kind("KafkaConsumerLag")
          .metadata(ResourceMetadata.builder()
              .resourceName(null)
              .self(forConsumerLag(
                  clusterId, consumerGroupId,
                  tp.topic(), tp.partition()).getRelated())
              .build())
          .clusterId(clusterId)
          .consumerGroupId(consumerGroupId)
          .topicName(tp.topic())
          .partitionId(tp.partition())
          .currentOffset(currentOffset)
          .logEndOffset(endOffset)
          .lag(lag)
          .consumerId(consumerId)
          .instanceId(instanceId)
          .clientId(clientId)
          .build());
    }
    return lagEntries;
  }

  private static ConsumerGroupData toConsumerGroupData(
      String clusterId, ConsumerGroupDescription description
  ) {
    return ConsumerGroupData.builder()
        .kind("KafkaConsumerGroup")
        .metadata(ResourceMetadata.builder()
            .resourceName(null)
            .self(forConsumerGroup(
                clusterId, description.groupId()).getRelated())
            .build())
        .clusterId(clusterId)
        .consumerGroupId(description.groupId())
        .isSimple(description.isSimpleConsumerGroup())
        .partitionAssignor(
            Optional.ofNullable(description.partitionAssignor()).orElse(""))
        .state(description.groupState().name())
        .coordinator(forController(clusterId, description.coordinator().id()))
        .consumer(forConsumers(clusterId, description.groupId()))
        .lagSummary(forConsumerGroupLagSummary(clusterId, description.groupId()))
        .build();
  }

  private static ConsumerData toConsumerData(
      String clusterId, String consumerGroupId, MemberDescription member
  ) {
    return ConsumerData.builder()
        .kind("KafkaConsumer")
        .metadata(ResourceMetadata.builder()
            .resourceName(null)
            .self(forConsumer(
                clusterId, consumerGroupId, member.consumerId()).getRelated())
            .build())
        .clusterId(clusterId)
        .consumerGroupId(consumerGroupId)
        .consumerId(member.consumerId())
        .instanceId(member.groupInstanceId().orElse(null))
        .clientId(member.clientId())
        .assignments(forConsumerAssignments(
            clusterId, consumerGroupId, member.consumerId()))
        .build();
  }

  private static ConsumerAssignmentData toConsumerAssignmentData(
      String clusterId, String consumerGroupId,
      String consumerId, TopicPartition tp
  ) {
    return ConsumerAssignmentData.builder()
        .kind("KafkaConsumerAssignment")
        .metadata(ResourceMetadata.builder()
            .resourceName(null)
            .self(forConsumerAssignment(
                clusterId, consumerGroupId, consumerId,
                tp.topic(), tp.partition()).getRelated())
            .build())
        .clusterId(clusterId)
        .consumerGroupId(consumerGroupId)
        .consumerId(consumerId)
        .topicName(tp.topic())
        .partitionId(tp.partition())
        .partition(forPartition(clusterId, tp.topic(), tp.partition()))
        .lag(forConsumerLag(clusterId, consumerGroupId, tp.topic(), tp.partition()))
        .build();
  }
}
