package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.idesidecar.restapi.resources.TemplateResource;
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
public class TemplateList extends BaseList<Template> {

  public TemplateList(List<Template> templateList) {
    super(
        templateList,
        CollectionMetadata.from(
            templateList.size(),
            TemplateResource.API_RESOURCE_PATH
        )
    );
  }

  @JsonCreator
  public TemplateList(
      @JsonProperty(value = "data", required = true) Template[] data,
      @JsonProperty(value = "metadata", required = true) CollectionMetadata metadata
  ) {
    super(
        List.of(data),
        metadata != null ? metadata.withTotalSize(data.length) : CollectionMetadata.from(
            data.length,
            TemplateResource.API_RESOURCE_PATH
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
    TemplateList that = (TemplateList) o;
    return Objects.equals(metadata, that.metadata)
        && Objects.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, metadata);
  }

  @Override
  public String toString() {
    return "TemplateList{" + "metadata=" + metadata + ", data=" + data + '}';
  }
}
