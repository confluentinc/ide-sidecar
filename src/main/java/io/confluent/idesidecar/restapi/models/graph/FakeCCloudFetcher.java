package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.models.graph.CloudProvider.AWS;

import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PageLimits;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link CCloudFetcher} that always returns fixed, consistent and fake CCloud resource objects.
 */
@ApplicationScoped
@RegisterForReflection
public class FakeCCloudFetcher implements CCloudFetcher {

  public static final String CONNECTION_1_ID = "ca3c1455-756d-46e4-97ea-c40a0ba64233";
  public static final String CONNECTION_2_ID = "08282440-66ec-4f70-9e6a-d379642c215c";

  public static final String DEVEL_ORG_ID = "23b1185e-d874-4f61-81d6-c9c61aa8969c";
  public static final String TEST_ORG_ID = "33f11e2e-c314-4e92-8e44-546ac834469b";
  public static final String PROD_ORG_ID = "7e468e7d-1efa-4f2d-bbc9-b45b100089e4";

  @RegisterForReflection
  static class Connection {

    final CCloudConnection delegate;
    final List<CCloudOrganization> orgs;
    final List<CCloudEnvironment> envs = new ArrayList<>();
    final Map<String, CCloudSchemaRegistry> srByEnv = new HashMap<>();
    final Map<String, List<CCloudKafkaCluster>> clustersByEnv = new HashMap<>();

    Connection(String id, String name, CCloudOrganization... orgs) {
      this.delegate = new CCloudConnection(id, name);
      this.orgs = Arrays.asList(orgs);
    }

    public String id() {
      return delegate.getId();
    }

    public CCloudConnection delegate() {
      return delegate;
    }

    public Connection addEnvironment(
        CCloudEnvironment env,
        CCloudSchemaRegistry registry,
        CCloudKafkaCluster... kafkaClusters
    ) {
      // Add the reference to the current organization
      var defOrg = orgs.stream().filter(
          CCloudOrganization::current).findFirst().orElse(null);
      envs.add(
          env.withOrganization(defOrg)
      );
      if (registry != null) {
        // Add the reference to the environment and org
        srByEnv.put(env.id(), registry.withEnvironment(env).withOrganization(defOrg));
      }
      // Add the reference to the environment and org
      var lkcs = Stream
          .of(kafkaClusters)
          .map(c -> c.withEnvironment(env).withOrganization(defOrg))
          .toList();
      clustersByEnv.put(env.id(), lkcs);
      return this;
    }

    public List<CCloudOrganization> getOrganizations() {
      return orgs;
    }

    public List<CCloudEnvironment> getEnvironments() {
      return envs;
    }

    public List<CCloudKafkaCluster> getKafkaClusters(String envId) {
      return clustersByEnv.get(envId);
    }

    public CCloudSchemaRegistry getSchemaRegistry(String envId) {
      return srByEnv.get(envId);
    }

    public List<CCloudKafkaCluster> findKafkaClusters(
        CCloudSearchCriteria criteria) {
      return clustersByEnv
          .values()
          .stream()
          .flatMap(Collection::stream)
          .filter(lkc -> lkc.matches(criteria))
          .toList();
    }
  }

  static final Connection CONNECTION_1 = new Connection(
      CONNECTION_1_ID,
      "Dev Account",
      new CCloudOrganization(DEVEL_ORG_ID, "Development Org", true),
      new CCloudOrganization(TEST_ORG_ID, "Test Org", false)
  ).addEnvironment(
      new CCloudEnvironment("env-1234", "Devel", CCloudGovernancePackage.ESSENTIALS),
      new CCloudSchemaRegistry("lsrc-1234", "psrc-12345", AWS, "us-west-1"),
      new CCloudKafkaCluster("lkc-k123", "sammy's cluster", AWS, "us-west-1", "pkc-kp123"),
      new CCloudKafkaCluster("lkc-k234", "dell's cluster", AWS, "us-west-2", "pkc-kp234")
  ).addEnvironment(
      new CCloudEnvironment("env-2345", "Test", CCloudGovernancePackage.ESSENTIALS),
      new CCloudSchemaRegistry("lsrc-4567", "psrc-4567", AWS, "us-west-1"),
      new CCloudKafkaCluster("lkc-s4567", "shared test cluster", AWS, "us-west-1", "pkc-s4567")
  );

  static final Connection CONNECTION_2 = new Connection(
      CONNECTION_2_ID,
      "Prod Account",
      new CCloudOrganization(PROD_ORG_ID, "Prod Org", true)
  ).addEnvironment(
      new CCloudEnvironment("env-3456", "Prod", CCloudGovernancePackage.ESSENTIALS),
      new CCloudSchemaRegistry("lsrc-3456", "psrc-3456", AWS, "us-west-2"),
      new CCloudKafkaCluster("lkc-x123", "service cluster", AWS, "us-west-2", "pkc-x123")
  );

  static final Map<String, Connection> CONNECTIONS = Stream
      .of(CONNECTION_1, CONNECTION_2)
      .collect(Collectors.toMap(Connection::id, c -> c, (x, y) -> y, LinkedHashMap::new));

  public Multi<CCloudConnection> getConnections() {
    return Multi.createFrom().iterable(
        CONNECTIONS.values().stream().map(Connection::delegate).collect(Collectors.toList())
    );
  }

  public Uni<CCloudConnection> getConnection(String connectionId) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Uni.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Uni.createFrom().item(connection.delegate());
  }

  public CCloudConnection getConnection(int index) {
    try {
      return CONNECTIONS.values()
          .stream()
          .toList()
          .get(index)
          .delegate();
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  public Multi<CCloudOrganization> getOrganizations(String connectionId) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Multi.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Multi.createFrom().iterable(connection.getOrganizations());
  }

  @Override
  public Multi<CCloudEnvironment> getEnvironments(String connectionId, PageLimits limits) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Multi.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Multi.createFrom().iterable(connection.getEnvironments());
  }

  @Override
  public Multi<CCloudKafkaCluster> getKafkaClusters(
      String connectionId, String envId, PageLimits limits) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Multi.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Multi.createFrom().iterable(connection.getKafkaClusters(envId));
  }

  public Uni<CCloudSchemaRegistry> getSchemaRegistry(String connectionId, String envId) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Uni.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Uni.createFrom().item(connection.getSchemaRegistry(envId));
  }

  @Override
  public Multi<CCloudKafkaCluster> findKafkaClusters(
      String connectionId,
      CCloudSearchCriteria criteria,
      PageLimits envLimits,
      PageLimits clusterLimits
  ) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Multi.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    return Multi.createFrom().iterable(connection.findKafkaClusters(criteria));
  }

  @Override
  public Multi<FlinkComputePool> getFlinkComputePools(String connectionId, String envId) {
    var connection = CONNECTIONS.get(connectionId);
    if (connection == null) {
      return Multi.createFrom().failure(
          new ConnectionNotFoundException(
              String.format("Connection %s is not found.", connectionId)
          )
      );
    }
    List<FlinkComputePool> flinkComputePools = List.of(
        new FlinkComputePool(
            "flink-1",
            new FlinkComputePoolSpec(
                "flink-1",
                "AWS",
                "us-east-2",
                10,
                new EnvironmentReference("id1", "resource1", "resource1"),
                new NetworkReference("id1", "env2", "related", "resource2")
            ),
            new FlinkComputePoolStatus("Active", 10)
        ),
        new FlinkComputePool(
            "flink-2",
            new FlinkComputePoolSpec(
                "flink-1",
                "AWS",
                "us-west-2",
                10,
                new EnvironmentReference("id2", "resource2", "resource2"),
                new NetworkReference("id2", "env2", "related", "resource2")
            ),
            new FlinkComputePoolStatus("PROVISIONING", 5)
        )
    );
    return Multi.createFrom().iterable(flinkComputePools);
  }

}
