/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.queryGraphQLRaw;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static java.util.function.Predicate.not;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.LocalConfig;
import io.confluent.idesidecar.restapi.models.ConnectionsList;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.quarkus.logging.Log;
import io.restassured.RestAssured;
import io.restassured.config.DecoderConfig;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

public interface SidecarClientApi {

  record SchemaRegistry(String connectionId, String id, String uri) {
  }

  record KafkaCluster(String connectionId, String id, String bootstrapServers) {
  }

  String sidecarHost();

  void createTopic(String topicName, int partitions, int replicationFactor);

  String currentConnectionId();

  void createTopic(String topicName);

  void deleteTopic(String topicName);

  void deleteAllTopics(String clusterId);

  Set<String> listTopics();

  Set<String> listSubjects(String srClusterId);

  void deleteAllSubjects(String srClusterId);

  void deleteSubject(String subject, String srClusterId);

  void deleteAllContent();

  void withCluster(String clusterId, Runnable action);

  <T> T fromCluster(String clusterId, Supplier<T> action);

  void useClusters(KafkaCluster kafkaCluster, SchemaRegistry schemaRegistry);

  void forEachConnection(ConnectionType type, Consumer<Connection> action);

  void forEachConnection(Consumer<Connection> action);

  void forEachConnection(Predicate<Connection> filter, Consumer<Connection> action);

  List<Connection> listConnections();

  Connection createLocalConnection(String schemaRegistryUri);

  void useConnection(String connectionId);

  void setCurrentCluster(String clusterId);

  Connection createConnection(ConnectionSpec spec);

  Connection createLocalConnectionTo(TestEnvironment env, String scope);

  Connection createDirectConnectionTo(TestEnvironment env, String scope);

  void deleteConnection(String connectionId);

  void deleteAllConnections();

  void deleteAllConnections(Predicate<Connection> filter);

  RequestSpecification givenDefault();

  RequestSpecification givenConnectionId();

  RequestSpecification givenConnectionId(String connectionId);

  Map<String, String> clusterIdPathParams();

  void shouldRaiseErrorWhenConnectionIdIsMissing(String path);

  void shouldRaiseErrorWhenConnectionNotFound(String path);

  void produceRecord(
      Integer partitionId,
      String topicName,
      Object key,
      Object value
  );

  void produceRecord(
      Integer partitionId,
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  );

  void produceRecord(
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  );

  ValidatableResponse produceRecordThen(
      Integer partitionId,
      String topicName,
      Object key,
      Object value
  );

  ValidatableResponse produceRecordThen(
      Integer partitionId,
      String topicName,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  );

  ProduceRequest createProduceRequest(
      Integer partitionId,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion
  );

  SimpleConsumeMultiPartitionResponse consume(
      String topicName, SimpleConsumeMultiPartitionRequest requestBody
  );

  /**
   * Produce plain old String key/value records to a topic
   */
  void produceStringRecords(String topicName, String[][] records);

  Schema createSchema(String subject, String schemaType, String schema);

  Schema getLatestSchemaVersion(String subject, String srClusterId);

  Optional<SchemaRegistry> getSchemaRegistryCluster();

  Optional<SchemaRegistry> getSchemaRegistryCluster(String query, String connectionId);

  Optional<KafkaCluster> getKafkaCluster();

  Optional<KafkaCluster> getKafkaCluster(String query, String connectionId);

  String randomTopicName();

  String loadCCloudConnectionsGraphQL();

  String loadDirectConnectionsGraphQL();

  String loadLocalConnectionsGraphQL();

  ValidatableResponse submitGraphQL(String query);

  ValidatableResponse submitLocalConnectionsGraphQL();

  ValidatableResponse submitDirectConnectionsGraphQL() ;

  ValidatableResponse submitCCloudConnectionsGraphQL();
}
