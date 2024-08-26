package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record CCloudSchemaRegistry(
    String id,
    String uri,
    CloudProvider provider,
    String region,
    CCloudReference organization,
    CCloudReference environment,
    String connectionId
) implements SchemaRegistry {

  public CCloudSchemaRegistry(
      String id,
      String url,
      CloudProvider provider,
      String region
  ) {
    this(
        id,
        url,
        provider,
        region,
        null,
        null,
        null
    );
  }

  public CCloudSchemaRegistry withEnvironment(CCloudEnvironment env) {
    if (env == null) {
      return this;
    }
    return new CCloudSchemaRegistry(
        id,
        uri,
        provider,
        region,
        organization,
        new CCloudReference(env.id(), env.name()),
        null
    );
  }

  public CCloudSchemaRegistry withOrganization(CCloudReference org) {
    if (org == null) {
      return this;
    }
    return new CCloudSchemaRegistry(
        id,
        uri,
        provider,
        region,
        org,
        environment,
        null
    );
  }

  public CCloudSchemaRegistry withOrganization(CCloudOrganization org) {
    if (org == null) {
      return this;
    }
    return new CCloudSchemaRegistry(
        id,
        uri,
        provider,
        region,
        new CCloudReference(org.id(), org.name()),
        environment,
        null
    );
  }

  public CCloudSchemaRegistry withConnectionId(String connectionId) {
    return new CCloudSchemaRegistry(
        id,
        uri,
        provider,
        region,
        organization,
        environment,
        connectionId
    );
  }

  public boolean matches(CCloudSearchCriteria criteria) {
    return criteria.environmentId().test(environment != null ? environment.id() : "")
           && criteria.provider().test(provider)
           && criteria.region().test(region);
  }
}
