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

class CCloudSchemaRegistryTest {

  private static final String ORG_ID = UUID.randomUUID().toString();

  private CCloudSchemaRegistry orig;
  private CCloudSchemaRegistry updated;

  @BeforeEach
  void setUp() {
    orig = new CCloudSchemaRegistry(
        "lsrc-1234",
        "https://psrc-1234.us-west-1.aws.confluent.cloud",
        CloudProvider.AWS,
        "us-west-1"
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

    assertSame(updated, updated.withOrganization((CCloudOrganization) null));
    assertSame(updated, updated.withOrganization((CCloudReference) null));
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
}
