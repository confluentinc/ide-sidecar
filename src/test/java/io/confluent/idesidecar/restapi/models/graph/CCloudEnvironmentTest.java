package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CCloudEnvironmentTest {

  private static final String ORG_ID = UUID.randomUUID().toString();

  private CCloudEnvironment orig;
  private CCloudEnvironment updated;

  @BeforeEach
  void setUp() {
    orig = new CCloudEnvironment(
        "env-1234",
        "My Environment",
        CCloudGovernancePackage.ESSENTIALS
    );
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

    assertSame(updated, updated.withOrganization(null));
  }
}
