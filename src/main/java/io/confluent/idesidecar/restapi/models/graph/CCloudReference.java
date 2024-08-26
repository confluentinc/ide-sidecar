package io.confluent.idesidecar.restapi.models.graph;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;
import io.smallrye.graphql.api.Nullable;

/**
 * A reference to another CCloud resource with a given ID and name.
 *
 * @param id   the identifier of the resource
 * @param name the name of the resource; may be null if it is not known
 */
@RegisterForReflection
@DefaultNonNull
public record CCloudReference(
    String id,
    @Nullable String name
) {

}
