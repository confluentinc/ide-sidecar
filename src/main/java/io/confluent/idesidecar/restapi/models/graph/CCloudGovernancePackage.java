package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public enum CCloudGovernancePackage {
  ESSENTIALS,
  ADVANCED,
  NONE;

  @Override
  public String toString() {
    return name().toLowerCase();
  }
}
