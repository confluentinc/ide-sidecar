/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestHeader;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface SidecarClientApi {

  record SchemaRegistry(String connectionId, String id, String uri) {

  }

  record KafkaCluster(String connectionId, String id, String bootstrapServers) {

  }

  String sidecarHost();

  default String sidecarUri(String endpointPath) {
    return "%s%s".formatted(sidecarHost(), endpointPath);
  }

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

  ValidatableResponse testConnectionWithResponse(ConnectionSpec spec);

  Connection testConnection(ConnectionSpec spec);

  Connection createConnection(ConnectionSpec spec);

  Connection createConnection(ConnectionSpec spec, boolean waitUntilConnected);

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
      Integer valueSchemaVersion,
      Set<ProduceRequestHeader> headers
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
      Integer valueSchemaVersion,
      Set<ProduceRequestHeader> headers
  );

  ValidatableResponse produceRecordThen(
      String topicName,
      ProduceRequest request
  );

  ValidatableResponse produceRecordThen(
      String topicName,
      ProduceRequest request,
      Boolean dryRun
  );

  ProduceRequest createProduceRequest(
      Integer partitionId,
      Object key,
      Integer keySchemaVersion,
      Object value,
      Integer valueSchemaVersion,
      Set<ProduceRequestHeader> headers
  );

  SimpleConsumeMultiPartitionResponse consume(
      String topicName, SimpleConsumeMultiPartitionRequest requestBody
  );

  /**
   * Produce plain old String key/value records to a topic
   */
  void produceStringRecords(String topicName, String[][] records);

  Schema createSchema(
      String subject, String schemaType, String schema, List<SchemaReference> references
  );

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

  ValidatableResponse submitDirectConnectionsGraphQL();

  ValidatableResponse submitCCloudConnectionsGraphQL();

  boolean localConnectionsGraphQLResponseContains(String connectionId);

  boolean directConnectionsGraphQLResponseContains(String connectionId);
}
