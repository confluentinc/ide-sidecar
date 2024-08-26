package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PageLimits;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class FakeCCloudFetcherTest {

  private FakeCCloudFetcher fetcher;

  @BeforeEach
  public void beforeEach() {
    fetcher = new FakeCCloudFetcher();
  }

  @Test
  void testGetConnection() {
    fetcher
        .getConnections()
        .subscribe()
        .withSubscriber(AssertSubscriber.create(2))
        .assertCompleted()
        .assertItems(
            FakeCCloudFetcher.CONNECTION_1.delegate(),
            FakeCCloudFetcher.CONNECTION_2.delegate());
  }

  @Test
  void shouldGetOrganizations() {
    fetcher
        .getOrganizations(FakeCCloudFetcher.CONNECTION_1_ID)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(2))
        .assertCompleted()
        .assertItems(
            new CCloudOrganization(FakeCCloudFetcher.DEVEL_ORG_ID, "Development Org", true),
            new CCloudOrganization(FakeCCloudFetcher.TEST_ORG_ID, "Test Org", false)
        );
    fetcher
        .getOrganizations(FakeCCloudFetcher.CONNECTION_2_ID)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(1))
        .assertCompleted()
        .assertItems(
            new CCloudOrganization(FakeCCloudFetcher.PROD_ORG_ID, "Prod Org", true)
        );

    fetcher
        .getOrganizations(null)
        .subscribe()
        .withSubscriber(AssertSubscriber.create())
        .assertFailedWith(ConnectionNotFoundException.class);
  }

  @Test
  void shouldGetEnvironmentsForConnection() {
    var connection1Environments = FakeCCloudFetcher.CONNECTION_1
        .getEnvironments().toArray(CCloudEnvironment[]::new);
    var connection2Environments = FakeCCloudFetcher.CONNECTION_2
        .getEnvironments().toArray(CCloudEnvironment[]::new);

    fetcher
        .getEnvironments(FakeCCloudFetcher.CONNECTION_1_ID, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection1Environments.length))
        .assertCompleted()
        .assertItems(connection1Environments);
    fetcher
        .getEnvironments(FakeCCloudFetcher.CONNECTION_2_ID, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection2Environments.length))
        .assertCompleted()
        .assertItems(connection2Environments);

    fetcher
        .getEnvironments(null, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create())
        .assertFailedWith(ConnectionNotFoundException.class);
  }

  @Test
  void shouldGetKafkaClusters() {
    var connection1KafkaClusters = FakeCCloudFetcher.CONNECTION_1
        .getKafkaClusters("env-1234").toArray(CCloudKafkaCluster[]::new);
    var connection2KafkaClusters = FakeCCloudFetcher.CONNECTION_2
        .getKafkaClusters("env-3456").toArray(CCloudKafkaCluster[]::new);
    fetcher
        .getKafkaClusters(FakeCCloudFetcher.CONNECTION_1_ID, "env-1234", PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection1KafkaClusters.length))
        .assertCompleted()
        .assertItems(connection1KafkaClusters);
    fetcher
        .getKafkaClusters(FakeCCloudFetcher.CONNECTION_2_ID, "env-3456", PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection2KafkaClusters.length))
        .assertCompleted()
        .assertItems(connection2KafkaClusters);

    fetcher
        .getKafkaClusters(null, "env-1234", PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create())
        .assertFailedWith(ConnectionNotFoundException.class);
  }

  @Test
  void shouldGetSchemaRegistry() {
    fetcher
        .getSchemaRegistry(FakeCCloudFetcher.CONNECTION_1_ID, "env-1234")
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .assertCompleted()
        .assertItem(
            FakeCCloudFetcher.CONNECTION_1.getSchemaRegistry("env-1234"));
    fetcher
        .getSchemaRegistry(FakeCCloudFetcher.CONNECTION_2_ID, "env-3456")
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .assertCompleted()
        .assertItem(
            FakeCCloudFetcher.CONNECTION_2.getSchemaRegistry("env-3456"));

    fetcher
        .getSchemaRegistry(null, "env-1234")
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .assertFailedWith(ConnectionNotFoundException.class);
  }

  @Test
  void findKafkaClusters() {
    var criteria = CCloudSearchCriteria.create().withCloudProviderContaining("aws");

    var connection1KafkaClusters = FakeCCloudFetcher.CONNECTION_1
        .findKafkaClusters(criteria).toArray(CCloudKafkaCluster[]::new);
    var connection2KafkaClusters = FakeCCloudFetcher.CONNECTION_2
        .findKafkaClusters(criteria).toArray(CCloudKafkaCluster[]::new);

    fetcher
        .findKafkaClusters(
            FakeCCloudFetcher.CONNECTION_1_ID, criteria, PageLimits.DEFAULT, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection1KafkaClusters.length))
        .assertCompleted()
        .assertItems(connection1KafkaClusters);
    fetcher
        .findKafkaClusters(
            FakeCCloudFetcher.CONNECTION_2_ID, criteria, PageLimits.DEFAULT, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create(connection2KafkaClusters.length))
        .assertCompleted()
        .assertItems(connection2KafkaClusters);

    fetcher
        .findKafkaClusters(null, criteria, PageLimits.DEFAULT, PageLimits.DEFAULT)
        .subscribe()
        .withSubscriber(AssertSubscriber.create())
        .assertFailedWith(ConnectionNotFoundException.class);
  }
}