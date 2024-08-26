package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

@RegisterForReflection
@DefaultNonNull
public record CCloudKafkaCluster(
    String id,
    String name,
    CloudProvider provider,
    String region,
    String uri,
    String bootstrapServers,
    CCloudReference organization,
    CCloudReference environment,
    String connectionId

) implements KafkaCluster {

  public CCloudKafkaCluster(
      String id,
      String name,
      CloudProvider provider,
      String region,
      String pkcId
  ) {
    this(
        id,
        name,
        provider,
        region,
        String.format("https://%s.%s.%s.confluent.cloud:443", pkcId, region, provider),
        String.format("%s.%s.%s.confluent.cloud:9092", pkcId, region, provider),
        null,
        null,
        null
    );
  }

  public CCloudKafkaCluster withEnvironment(CCloudEnvironment env) {
    if (env == null) {
      return this;
    }
    return new CCloudKafkaCluster(
        id,
        name,
        provider,
        region,
        uri,
        bootstrapServers,
        organization,
        new CCloudReference(env.id(), env.name()),
        null
    );
  }

  public CCloudKafkaCluster withOrganization(CCloudReference org) {
    if (org == null) {
      return this;
    }
    return new CCloudKafkaCluster(
        id,
        name,
        provider,
        region,
        uri,
        bootstrapServers,
        org,
        environment,
        null
    );
  }

  public CCloudKafkaCluster withOrganization(CCloudOrganization org) {
    if (org == null) {
      return this;
    }
    return new CCloudKafkaCluster(
        id,
        name,
        provider,
        region,
        uri,
        bootstrapServers,
        new CCloudReference(org.id(), org.name()),
        environment,
        null
    );
  }

  public CCloudKafkaCluster withConnectionId(String connectionId) {
    return new CCloudKafkaCluster(
        id,
        name,
        provider,
        region,
        uri,
        bootstrapServers,
        organization,
        environment,
        connectionId
    );
  }

  public boolean matches(CCloudSearchCriteria criteria) {
    return criteria.name().test(name)
        && criteria.environmentId().test(environment != null ? environment.id() : "")
        && criteria.provider().test(provider)
        && criteria.region().test(region);
  }
}
