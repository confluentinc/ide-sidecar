package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.models.ClusterType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaRestUtilTest {


  @Test
  void testConstructResourceNameWithTopic() {
    String result = KafkaRestUtil.constructResourceName(ClusterType.KAFKA, "cluster1", "topic1");
    assertEquals("crn://kafka/resource=cluster1/sub-resource=topic1", result);

    result = KafkaRestUtil.constructResourceName(ClusterType.SCHEMA_REGISTRY, "cluster2", "topic2");
    assertEquals("crn://schema-registry/resource=cluster2/sub-resource=topic2", result);
  }

  @Test
  void testConstructResourceNameWithoutTopic() {
    String result = KafkaRestUtil.constructResourceName(ClusterType.KAFKA, "cluster1");
    assertEquals("crn://kafka/resource=cluster1", result);

    result = KafkaRestUtil.constructResourceName(ClusterType.SCHEMA_REGISTRY, "cluster2");
    assertEquals("crn://schema-registry/resource=cluster2", result);
  }
}