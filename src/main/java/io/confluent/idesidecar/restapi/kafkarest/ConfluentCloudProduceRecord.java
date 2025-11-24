package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.Base64;
import java.util.Map;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

@ApplicationScoped
public class ConfluentCloudProduceRecord extends GenericProduceRecord {

  private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

  // These are the types supported by Confluent Cloud Kafka REST
  private static final String BINARY_TYPE = "BINARY";
  private static final String JSON_TYPE = "JSON";

  @Inject
  @Named("ccloudProduceProcessor")
  Processor<KafkaRestProxyContext<ProduceRequest, ProduceResponse>,
      Future<KafkaRestProxyContext<ProduceRequest, ProduceResponse>>> ccloudProduceProcessor;

  @Override
  protected Uni<ProduceContext> sendSerializedRecord(ProduceContext c) {
    var produceRequest = new ProduceRequest(
        c.produceRequest().partitionId(),
        c.produceRequest().headers(),
        null,
        null,
        null
    );

    // Set the record key, while guarding against a NullPointerException
    if (c.keySchema().isPresent()) {
      produceRequest = produceRequest.withKey(
          new ProduceRequestData(
              BINARY_TYPE,
              null,
              null,
              null,
              null,
              null,
              encodeBase64OrNull(c.serializedKey() == null ? null : c.serializedKey().toByteArray())
          )
      );
    } else {
      produceRequest = produceRequest.withKey(
          new ProduceRequestData(
              JSON_TYPE,
              null,
              null,
              null,
              null,
              null,
              deserializeJson(c.serializedKey() == null ? null : c.serializedKey().toByteArray(), true)
          )
      );
    }

    // Set the record value, while guarding against a NullPointerException
    if (c.valueSchema().isPresent()) {
      produceRequest = produceRequest.withValue(
          new ProduceRequestData(
              BINARY_TYPE,
              null,
              null,
              null,
              null,
              null,
              encodeBase64OrNull(c.serializedValue() == null ? null : c.serializedValue().toByteArray())
          )
      );
    } else {
      produceRequest = produceRequest.withValue(
          new ProduceRequestData(
              JSON_TYPE,
              null,
              null,
              null,
              null,
              null,
              deserializeJson(c.serializedValue() == null ? null : c.serializedValue().toByteArray(), false)
          )
      );
    }

    final var finalProduceRequest = produceRequest;
    return uniStage(
        () -> ccloudProduceProcessor.process(new KafkaRestProxyContext<>(
            c.connectionId(),
            c.clusterId(),
            c.topicName(),
            finalProduceRequest
        )).toCompletionStage()
    ).map(kafkaRestProxyContext -> c
        .with()
        .recordMetadata(convertToRecordMetadata(kafkaRestProxyContext.getResponse()))
        .build()
    );
  }

  private RecordMetadata convertToRecordMetadata(ProduceResponse response) {
    return new RecordMetadata(
        new TopicPartition(response.getTopicName(), response.getPartitionId()),
        response.getOffset(),
        // batchIndex is ignored if baseOffset is set, which we expect to be set for sure
        -1,
        response.getTimestamp().getTime(),
        response.getKey().getSize().intValue(),
        response.getValue().getSize().intValue()
    );
  }

  private Object deserializeJson(byte[] serializedJsonBytes, boolean isKey) {
    if (serializedJsonBytes == null) {
      return null;
    }
    try (var kafkaJsonDeserializer = new KafkaJsonDeserializer<>()) {
      kafkaJsonDeserializer.configure(Map.of(), isKey);
      return kafkaJsonDeserializer.deserialize(null, serializedJsonBytes);
    }
  }

  private String encodeBase64OrNull(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    return BASE64_ENCODER.encodeToString(bytes);
  }
}
