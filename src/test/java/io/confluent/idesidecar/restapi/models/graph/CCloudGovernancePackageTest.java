package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CCloudGovernancePackageTest {

  @Test
  void shouldConvertToLowercase() {
    assertEquals("essentials", CCloudGovernancePackage.ESSENTIALS.toString());
    assertEquals("advanced", CCloudGovernancePackage.ADVANCED.toString());
    assertEquals("none", CCloudGovernancePackage.NONE.toString());
  }
}
