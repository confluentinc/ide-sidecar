package io.confluent.idesidecar.restapi.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStates;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.graph.CCloudEnvironment;
import io.confluent.idesidecar.restapi.models.graph.CCloudGovernancePackage;
import io.confluent.idesidecar.restapi.models.graph.CCloudKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.CCloudOrganization;
import io.confluent.idesidecar.restapi.models.graph.CCloudSchemaRegistry;
import io.confluent.idesidecar.restapi.models.graph.CloudProvider;
import io.confluent.idesidecar.restapi.models.graph.ClusterEvent;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.RealCCloudFetcher;
import io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ClusterCacheTest {

  @InjectMock
  RealCCloudFetcher ccloud;

  @InjectMock
  RealLocalFetcher local;

  @Inject
  ClusterCache cache;

  @BeforeEach
  void beforeEach() {
    cache.clear();
  }

  @Test
  void shouldNotFindConnectionWhenEmpty() {
    // When there are no connections in the cluster
    assertNoConnections();

    // Then getting the connection cache for an ID will fail
    assertNoConnection("123");

    // and no fetchers were used
    verifyNoInteractions(ccloud);
    verifyNoInteractions(local);
  }

  @Nested
  class CCloudClusters {

    static final String CONNECTION_1_ID = "c1";
    static final ConnectionState CONNECTION_1 = ConnectionStates.from(
        new ConnectionSpec(
            CONNECTION_1_ID,
            "Conn1",
            ConnectionSpec.ConnectionType.CCLOUD
        ),
        null
    );

    static final String CONNECTION_2_ID = "c2";
    static final ConnectionState CONNECTION_2 = ConnectionStates.from(
        new ConnectionSpec(
            CONNECTION_2_ID,
            "Conn2",
            ConnectionSpec.ConnectionType.CCLOUD
        ),
        null
    );

    static final CCloudOrganization ORG_1 = new CCloudOrganization(
        "org1",
        "my-org",
        false,
        CONNECTION_1_ID
    );

    static final CCloudOrganization ORG_2 = new CCloudOrganization(
        "org2",
        "my-other-org",
        false,
        CONNECTION_2_ID
    );
    static final CCloudEnvironment ENV_1 = new CCloudEnvironment(
        "env-123",
        "my-env",
        CCloudGovernancePackage.ESSENTIALS
    ).withOrganization(ORG_1).withConnectionId(CONNECTION_1_ID);

    static final CCloudEnvironment ENV_2 = new CCloudEnvironment(
        "env-456",
        "my-env-2",
        CCloudGovernancePackage.ESSENTIALS
    ).withOrganization(ORG_2).withConnectionId(CONNECTION_2_ID);
    static final CCloudKafkaCluster LKC_1 = new CCloudKafkaCluster(
        "lkc-1",
        "my-kafka-cluster",
        CloudProvider.AWS,
        "us-west-2",
        "pkc-123"
    ).withEnvironment(ENV_1).withOrganization(ORG_1).withConnectionId(CONNECTION_1_ID);

    static final CCloudKafkaCluster LKC_2 = new CCloudKafkaCluster(
        "lkc-2",
        "my-other-kafka-cluster",
        CloudProvider.AWS,
        "us-west-2",
        "pkc-456"
    ).withEnvironment(ENV_2).withOrganization(ORG_2).withConnectionId(CONNECTION_2_ID);
    static final CCloudSchemaRegistry SR_1 = new CCloudSchemaRegistry(
        "lkc-1",
        "http://something.confluent.cloud",
        CloudProvider.AWS,
        "us-west-2"
    ).withEnvironment(ENV_1).withOrganization(ORG_1).withConnectionId(CONNECTION_1_ID);

    @Test
    void shouldFindClusterCacheForConnectionThatWasCreated() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // Then there will be a connection cache
      assertConnection(CONNECTION_1);

      // but not for other connections
      assertNoConnection("123");

      // and no other methods are called
      verifyNoInteractions(ccloud);
      verifyNoInteractions(local);
    }

    @Test
    void shouldFindAlreadyLoadedKafkaClusterForConnectionButNoSchemaRegistry() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // and a Kafka cluster is loaded
      cache.onLoadingKafkaCluster(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, LKC_1)
      );

      // Then there will be a connection cache
      var clusters = assertConnection(CONNECTION_1);

      // that contains the Kafka cluster
      assertEquals(LKC_1, clusters.getKafkaCluster(LKC_1.id()));

      // But when no Schema Registry exists the environment
      expectGetSchemaRegistryForEnvironment(CONNECTION_1_ID, ENV_1.id(), null);

      // then none will be found
      assertThrows(
          ClusterNotFoundException.class,
          () -> clusters.getSchemaRegistryForKafkaCluster(LKC_1)
      );

      // and no other methods are called
      verifyNoInteractions(local);
    }

    @Test
    void shouldFindAlreadyLoadedKafkaClusterForConnectionAndLazilyLoadSchemaRegistry() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // and a Kafka cluster is loaded
      cache.onLoadingKafkaCluster(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, LKC_1)
      );

      // Then there will be a connection cache
      var clusters = assertConnection(CONNECTION_1);

      // that contains the Kafka cluster
      assertEquals(LKC_1, clusters.getKafkaCluster(LKC_1.id()));

      // But when a Schema Registry exists the environment
      expectGetSchemaRegistryForEnvironment(CONNECTION_1_ID, ENV_1.id(), SR_1);

      // then that SR cluster will be found
      assertEquals(SR_1, clusters.getSchemaRegistryForKafkaCluster(LKC_1));

      // and no other methods are called
      verifyNoInteractions(local);
    }

    @Test
    void shouldFindAlreadyLoadedKafkaClusterAndSchemaRegistryForConnection() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // and a Kafka cluster is loaded
      cache.onLoadingKafkaCluster(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, LKC_1)
      );

      // and a Schema Registry is loaded
      cache.onLoadingSchemaRegistry(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, SR_1)
      );

      // Then there will be a connection cache
      var clusters = assertConnection(CONNECTION_1);

      // that contains the Kafka cluster
      assertEquals(LKC_1, clusters.getKafkaCluster(LKC_1.id()));

      // and contains the SR cluster
      assertEquals(SR_1, clusters.getSchemaRegistryForKafkaCluster(LKC_1));

      // and no other methods are called
      verifyNoInteractions(ccloud);
      verifyNoInteractions(local);
    }

    @Test
    void shouldFindAlreadyLoadedSchemaRegistryForConnectionAndLazilyLoadKafkaCluster() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // and a Schema Registry is loaded
      cache.onLoadingSchemaRegistry(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, SR_1)
      );

      // But the Kafka Cluster will be loaded lazily
      expectFindKafkaCluster(CONNECTION_1_ID, LKC_1.id(), LKC_1);

      // Then there will be a connection cache
      var clusters = assertConnection(CONNECTION_1);

      // and contains the SR cluster
      assertEquals(SR_1, clusters.getSchemaRegistryForKafkaCluster(LKC_1));

      // then the cache will find the Kafka cluster
      assertEquals(LKC_1, clusters.getKafkaCluster(LKC_1.id()));

      // and no other methods are called
      verifyNoInteractions(local);
    }

    @Test
    void shouldRemoveCacheOnConnectionUpdate() {

      // When two connections are created
      cache.onConnectionCreated(CONNECTION_1);
      cache.onConnectionCreated(CONNECTION_2);

      // and a Kafka cluster is loaded in each connection
      cache.onLoadingKafkaCluster(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, LKC_1)
      );
      cache.onLoadingKafkaCluster(
          new ClusterEvent(CONNECTION_2_ID, ConnectionSpec.ConnectionType.CCLOUD, LKC_2)
      );

      // Then there will be a connection cache for both connections
      assertConnection(CONNECTION_1);
      assertConnection(CONNECTION_2);

      // and the cache for each connection contains their Kafka cluster
      assertKafkaCluster(LKC_1);
      assertKafkaCluster(LKC_2);

      // When one of the connections is updated
      cache.onConnectionUpdated(CONNECTION_1);

      // then the cache for the connection will have been cleared (no cluster will be found)
      var clusters = assertConnection(CONNECTION_1);
      assertThrows(
          ClusterNotFoundException.class,
          () -> clusters.getKafkaCluster(LKC_1.id(), false)
      );

      // but the cache for the other connection is unaffected
      assertConnection(CONNECTION_2);
      assertKafkaCluster(LKC_2);

      // and the Kafka cluster for connection 1 will be loaded again
      expectFindKafkaCluster(CONNECTION_1_ID, LKC_1.id(), LKC_1);

      // when the Kafka cluster is accessed again
      assertEquals(LKC_1, clusters.getKafkaCluster(LKC_1.id()));
    }

    @Test
    void shouldNotFindAlreadyLoadedSchemaRegistryForKafkaClusterThatDoesNotExist() {
      // When a connection is created
      cache.onConnectionCreated(CONNECTION_1);

      // and a Schema Registry is loaded
      cache.onLoadingSchemaRegistry(
          new ClusterEvent(CONNECTION_1_ID, ConnectionSpec.ConnectionType.CCLOUD, SR_1)
      );

      // But no Kafka Cluster will be loaded lazily
      expectFindKafkaCluster(CONNECTION_1_ID, LKC_1.id(), null);

      // Then there will be a connection cache
      var clusters = assertConnection(CONNECTION_1);

      // and the SR cluster will not be found
      assertThrows(
          ClusterNotFoundException.class,
          () -> clusters.getSchemaRegistryForKafkaCluster(LKC_1)
      );

      // and the Kafka cluster will not be found
      assertThrows(
          ClusterNotFoundException.class,
          () -> clusters.getKafkaCluster(LKC_1.id())
      );

      // and no other methods are called
      verifyNoInteractions(local);
    }

    void expectFindKafkaCluster(
        String connectionId,
        String lkcId,
        CCloudKafkaCluster returns
    ) {
      when(
          ccloud.findKafkaCluster(connectionId, lkcId)
      ).thenReturn(
          Uni.createFrom().item(returns)
      );
    }

    void expectGetSchemaRegistryForEnvironment(
        String connectionId,
        String env,
        CCloudSchemaRegistry returns
    ) {
      when(
          ccloud.getSchemaRegistry(connectionId, env)
      ).thenReturn(
          Uni.createFrom().item(returns)
      );
    }
  }


  void assertNoConnections() {
    assertEquals(0, cache.clustersByConnectionId.size());
  }

  void assertNoConnection(String id) {
    assertThrows(
        ConnectionNotFoundException.class,
        () -> cache.forConnection(id)
    );
  }

  ClusterCache.Clusters assertConnection(ConnectionState state) {
    return assertConnection(state.getSpec().id());
  }

  ClusterCache.Clusters assertConnection(String id) {
    var result = cache.forConnection(id);
    assertNotNull(result);
    return result;
  }

  void assertKafkaCluster(KafkaCluster cluster) {
    var clusters = cache.forConnection(cluster.connectionId());
    assertNotNull(clusters);
    assertEquals(
        cluster,
        clusters.getKafkaCluster(cluster.id())
    );
  }
}