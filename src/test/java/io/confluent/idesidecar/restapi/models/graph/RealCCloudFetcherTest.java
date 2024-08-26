package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PageLimits;
import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PaginationState;
import io.quarkus.test.junit.QuarkusTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class RealCCloudFetcherTest {

  private static final String DEVELOPMENT_ORG_ID = "23b1185e-d874-4f61-81d6-c9c61aa8969c";

  @Test
  void shouldParseOrgListResponse() {
    var fetcher = new RealCCloudFetcher();
    var state = new PaginationState("", PageLimits.DEFAULT);
    var orgList = fetcher.parseOrgList(loadResource(
        "ccloud-resources-mock-responses/list-organizations.json"), state);
    assertEquals(3, orgList.items().size());
    assertEquals(
        new CCloudOrganization(DEVELOPMENT_ORG_ID, "Development Org", false),
        orgList.items().getFirst()
    );
  }

  @Test
  void shouldParseEnvListResponse() {
    var fetcher = new RealCCloudFetcher();
    var state = new PaginationState("", PageLimits.DEFAULT);
    var envList = fetcher.parseEnvList(loadResource(
        "ccloud-resources-mock-responses/list-environments.json"), state);
    assertEquals(2, envList.items().size());
    assertEquals(
        Stream.of(
            new CCloudEnvironment("env-x7727g", "main-test-env", CCloudGovernancePackage.NONE),
            new CCloudEnvironment(
                "env-kkk3jg", "env-with-no-kafka-cluster", CCloudGovernancePackage.ESSENTIALS)
        ).map(env -> env.withOrganization(
            new CCloudOrganization(DEVELOPMENT_ORG_ID, null, false)
        )).toList(),
        envList.items()
    );
  }
}
