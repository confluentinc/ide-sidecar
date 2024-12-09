package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.Optional;

public enum SubjectNameStrategyEnum {
  TOPIC_NAME("topic_name", new TopicNameStrategy()),
  TOPIC_RECORD_NAME("topic_record_name", new TopicRecordNameStrategy()),
  RECORD_NAME("record_name", new RecordNameStrategy());

  private final String value;
  private final SubjectNameStrategy strategy;

  SubjectNameStrategyEnum(String value, SubjectNameStrategy strategy) {
    this.value = value;
    this.strategy = strategy;
  }

  public static Optional<SubjectNameStrategyEnum> parse(String subjectNameStrategy) {
    for (var r : SubjectNameStrategyEnum.values()) {
      if (r.value.equalsIgnoreCase(subjectNameStrategy)) {
        return Optional.of(r);
      }
    }
    return Optional.empty();
  }

  public String className() {
    return strategy.getClass().getName();
  }

  public String subjectName(
      String topic, boolean isKey, ParsedSchema schema
  ) {
    return strategy.subjectName(topic, isKey, schema);
  }
}
