package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

import java.util.Optional;

public enum SubjectNameStrategyEnum {
  TOPIC_NAME("topic_name", TopicNameStrategy.class.getName()),
  TOPIC_RECORD_NAME("topic_record_name", TopicRecordNameStrategy.class.getName()),
  RECORD_NAME("record_name", RecordNameStrategy.class.getName());

  private final String value;
  public final String strategyClassName;

  SubjectNameStrategyEnum(String value, String strategyClassName) {
    this.value = value;
    this.strategyClassName = strategyClassName;
  }

  public static Optional<SubjectNameStrategyEnum> parse(String subjectNameStrategy) {
    for (var r : SubjectNameStrategyEnum.values()) {
      if (r.value.equalsIgnoreCase(subjectNameStrategy)) {
        return Optional.of(r);
      }
    }
    return Optional.empty();
  }
}
