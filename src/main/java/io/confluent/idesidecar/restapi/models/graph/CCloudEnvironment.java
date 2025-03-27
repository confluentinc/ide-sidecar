package io.confluent.idesidecar.restapi.models.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext.OrganizationDetails;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
@JsonIgnoreProperties(ignoreUnknown = true)
public record CCloudEnvironment(
    String id,
    String name,
    CCloudGovernancePackage governancePackage,
    CCloudReference organization,
    String connectionId
) {

  public CCloudEnvironment(
      String id,
      String name,
      CCloudGovernancePackage governancePackage
  ) {
    this(id, name, governancePackage, null, null);
  }

  public CCloudEnvironment(
      String id,
      String name,
      CCloudReference organization,
      CCloudGovernancePackage governancePackage
  ) {
    this(id, name, governancePackage, organization, null);
  }

  public CCloudEnvironment withOrganization(CCloudOrganization org) {
    if (org == null) {
      return this;
    }
    return new CCloudEnvironment(
        id,
        name,
        governancePackage,
        new CCloudReference(org.id(), org.name()),
        connectionId
    );
  }

  public CCloudEnvironment withConnectionId(String connectionId) {
    return new CCloudEnvironment(
        id,
        name,
        governancePackage,
        organization,
        connectionId
    );
  }

  public CCloudEnvironment withCurrentOrganization(OrganizationDetails details) {
    if (details == null) {
      return this;
    }
    return new CCloudEnvironment(
        id,
        name,
        governancePackage,
        new CCloudReference(details.resourceId(), details.name()),
        connectionId
    );
  }
}