package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.idesidecar.restapi.resources.ConnectionsResource;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "api_version",
    "kind",
    "metadata",
    "data"
})
@RegisterForReflection
public class ConnectionsList extends BaseList<Connection> {

  public ConnectionsList() {
    this(List.of(), null);
  }

  public ConnectionsList(
      List<Connection> connections
  ) {
    this(connections, null);
  }

  public ConnectionsList(
      List<Connection> connections,
      CollectionMetadata metadata
  ) {
    super(
        connections,
        metadata != null ? metadata : CollectionMetadata.from(
            connections.size(),
            ConnectionsResource.API_RESOURCE_PATH
        )
    );
  }

  @JsonCreator
  public ConnectionsList(
      @JsonProperty(value = "data", required = true) Connection[] data,
      @JsonProperty(value = "metadata", required = true) CollectionMetadata metadata
  ) {
    this(
        List.of(data),
        metadata != null ? metadata.withTotalSize(data.length) : CollectionMetadata.from(
            data.length,
            ConnectionsResource.API_RESOURCE_PATH
        )
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConnectionsList that = (ConnectionsList) o;
    return Objects.equals(metadata, that.metadata)
           && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, metadata);
  }

  @Override
  public String toString() {
    return "ConnectionList{" + "metadata=" + metadata + ", data=" + data + '}';
  }
}
