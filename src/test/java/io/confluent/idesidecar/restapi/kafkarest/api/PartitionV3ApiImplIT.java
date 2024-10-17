package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class PartitionV3ApiImplIT extends ConfluentLocalTestBed {
  @Test
  void shouldListTopicPartitions() {
    createTopic("topic-multiple-partitions", 3, (short) 1);

    givenDefault()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/topic-multiple-partitions/partitions")
        .then()
        .body("data.size()", equalTo(3));
  }

  @Test
  void shouldGetTopicPartition() {
    createTopic("topic-single-partition", 1, (short) 1);

    givenDefault()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/topic-single-partition/partitions/0")
        .then()
        .body("partition_id", equalTo(0))
        .body("topic_name", equalTo("topic-single-partition"));
  }

  @Test
  void shouldThrow404WhenGettingPartitionForNonExistentTopic() {
    givenDefault()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/test-non-existent/partitions/0")
        .then()
        .statusCode(404);

    // Also check for the list partitions
    givenDefault()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/test-non-existent/partitions")
        .then()
        .statusCode(404);
  }

  @Test
  void shouldThrow404WhenGettingNonExistentPartition() {
    createTopic("topic-single-partition", 1, (short) 1);

    givenDefault()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/topic-single-partition/partitions/3")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo(
            "This server does not host topic-partition 3 for topic topic-single-partition"
        ));
  }

  @Test
  void shouldRaiseErrorWhenConnectionNotFound() {
    shouldRaiseErrorWhenConnectionNotFound(
        "/internal/kafka/v3/clusters/%s/topics/my-topic/partitions".formatted(CLUSTER_ID)
    );
  }

  @Test
  void shouldRaiseErrorWhenConnectionIdIsMissing() {
    shouldRaiseErrorWhenConnectionIdIsMissing(
        "/internal/kafka/v3/clusters/%s/topics/my-topic/partitions".formatted(CLUSTER_ID)
    );
  }
}