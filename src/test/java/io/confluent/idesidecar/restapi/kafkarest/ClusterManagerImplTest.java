package io.confluent.idesidecar.restapi.kafkarest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.clients.AdminClients;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ClusterManagerImpl}'s handling of a null or empty
 * {@code DescribeCluster.controller()}. WarpStream reports no controller for its playground
 * clusters, so this guards that case directly, independent of the WarpStream agent version that
 * {@code WarpStreamRegressionIT} happens to run against.
 */
@QuarkusTest
class ClusterManagerImplTest {

  private static final String CONNECTION_ID = "connection-id";
  private static final String CLUSTER_ID = "cluster-1";
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private DescribeClusterResult describeClusterResult;
  private ClusterManagerImpl clusterManager;

  @BeforeEach
  void setUp() {
    var adminClients = mock(AdminClients.class);
    var adminClient = mock(AdminClient.class);
    describeClusterResult = mock(DescribeClusterResult.class);

    when(adminClients.getClient(CONNECTION_ID, CLUSTER_ID)).thenReturn(adminClient);
    when(adminClient.describeCluster()).thenReturn(describeClusterResult);
    when(describeClusterResult.clusterId()).thenReturn(KafkaFuture.completedFuture(CLUSTER_ID));
    when(describeClusterResult.nodes())
        .thenReturn(KafkaFuture.<Collection<Node>>completedFuture(List.of()));

    clusterManager = new ClusterManagerImpl();
    clusterManager.adminClients = adminClients;
    clusterManager.connectionId = () -> CONNECTION_ID;
  }

  @Test
  void getKafkaClusterShouldOmitControllerWhenControllerIsNull() {
    when(describeClusterResult.controller()).thenReturn(KafkaFuture.<Node>completedFuture(null));

    var cluster = clusterManager.getKafkaCluster(CLUSTER_ID).await().atMost(TIMEOUT);

    assertNull(cluster.getController());
  }

  @Test
  void getKafkaClusterShouldOmitControllerWhenControllerIsEmpty() {
    when(describeClusterResult.controller())
        .thenReturn(KafkaFuture.completedFuture(Node.noNode()));

    var cluster = clusterManager.getKafkaCluster(CLUSTER_ID).await().atMost(TIMEOUT);

    assertNull(cluster.getController());
  }

  @Test
  void getKafkaClusterShouldIncludeControllerWhenControllerIsPresent() {
    var controller = new Node(7, "broker-7", 9092);
    when(describeClusterResult.controller())
        .thenReturn(KafkaFuture.completedFuture(controller));

    var cluster = clusterManager.getKafkaCluster(CLUSTER_ID).await().atMost(TIMEOUT);

    assertNotNull(cluster.getController());
    assertTrue(cluster.getController().getRelated().endsWith("/brokers/7"));
  }
}
