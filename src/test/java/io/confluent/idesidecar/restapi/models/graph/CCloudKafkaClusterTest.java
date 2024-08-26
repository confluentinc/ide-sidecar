package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CCloudKafkaClusterTest {

  private static final String ORG_ID = UUID.randomUUID().toString();

  private CCloudKafkaCluster orig;
  private CCloudKafkaCluster updated;

  @BeforeEach
  void setUp() {
    orig = new CCloudKafkaCluster(
        "lkc-1234",
        "My Cluster",
        CloudProvider.AWS,
        "us-west-1",
        "pkc-1234"
    );
  }

  @Test
  void shouldSetEnvironment() {
    // When an SR object has no environment
    assertNull(orig.environment());

    // Then adding an environment will return the new environment
    var env = new CCloudEnvironment(
        "env-123",
        "My Environment",
        CCloudGovernancePackage.ESSENTIALS
    );
    updated = orig.withEnvironment(env);
    assertNotNull(updated.environment());
    assertEquals(env.id(), updated.environment().id());
    assertEquals(env.name(), updated.environment().name());

    assertTrue(
        updated.matches(
            CCloudSearchCriteria.create().withEnvironmentIdContaining("env-123")
        )
    );
    assertFalse(
        updated.matches(
            CCloudSearchCriteria.create().withEnvironmentIdContaining("env-456")
        )
    );

    assertSame(updated, updated.withEnvironment(null));
  }

  @Test
  void shouldSetOrganization() {
    // When an SR object initially has no organization
    assertNull(orig.organization());

    // Then adding an environment will return the new environment
    var org = new CCloudOrganization(
        ORG_ID,
        "My Organization",
        false
    );
    updated = orig.withOrganization(org);
    assertNotNull(updated.organization());
    assertEquals(org.id(), updated.organization().id());
    assertEquals(org.name(), updated.organization().name());

    assertSame(updated, updated.withOrganization((CCloudReference) null));
    assertSame(updated, updated.withOrganization((CCloudOrganization) null));
  }

  @Test
  void shouldMatchCriteriaWithCloudProvider() {
    assertTrue(
        orig.matches(
            CCloudSearchCriteria.create().withCloudProviderContaining("aws")
        )
    );
    assertFalse(
        orig.matches(
            CCloudSearchCriteria.create().withCloudProviderContaining("acme")
        )
    );
  }

  @Test
  void shouldMatchCriteriaWithCloudRegion() {
    assertTrue(
        orig.matches(
            CCloudSearchCriteria.create().withRegionContaining("us-west")
        )
    );
    assertFalse(
        orig.matches(
            CCloudSearchCriteria.create().withRegionContaining("acme")
        )
    );
  }

  @Test
  void shouldMatchCriteriaWithName() {
    assertTrue(
        orig.matches(
            CCloudSearchCriteria.create().withNameContaining("cluster")
        )
    );
    assertFalse(
        orig.matches(
            CCloudSearchCriteria.create().withNameContaining("acme")
        )
    );
  }
}
