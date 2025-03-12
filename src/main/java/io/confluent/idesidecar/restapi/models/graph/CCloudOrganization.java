package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext.OrganizationDetails;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record CCloudOrganization(
    String id,
    String name,
    boolean current,
    String connectionId
) {

  public CCloudOrganization(String id, String name, boolean current) {
    this(id, name, current, null);
  }

  public CCloudOrganization withConnectionId(String connectionId) {
    return new CCloudOrganization(id, name, current, connectionId);
  }

  public CCloudOrganization withCurrentOrganization(OrganizationDetails details) {
    if (details == null) {
      return this;
    }
    return new CCloudOrganization(
        id,
        name,
        id.equals(details.resourceId()),
        connectionId
    );
  }
}
