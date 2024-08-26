package io.confluent.idesidecar.restapi.models.graph;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.graphql.api.DefaultNonNull;

/**
 * A GraphQL object representing a {@link io.confluent.idesidecar.restapi.models.Connection}.
 * A subtype is used for each kind of connection, and all fields are immutable.
 */
@RegisterForReflection
@DefaultNonNull
public abstract class Connection {

  private final String id;
  private final String name;
  private final ConnectionType type;

  protected Connection(String id, String name, ConnectionType type) {
    this.id = id;
    this.name = name;
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  /**
   * The type of connection, which corresponds to a specific subtype of this class.
   * @return the type; never null
   */
  public ConnectionType getType() {
    return type;
  }
}
