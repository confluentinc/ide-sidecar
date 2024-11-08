package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.util.CCloud;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.graphql.api.DefaultNonNull;
import java.util.Optional;

@RegisterForReflection
@DefaultNonNull
public interface SchemaRegistry extends Cluster {
  @NotNull String uri();

  default Optional<CCloud.LsrcId> logicalId() {
    return CCloud.SchemaRegistryIdentifier
        .parse(uri())
        .map(CCloud.LsrcId.class::cast);
  }
}
