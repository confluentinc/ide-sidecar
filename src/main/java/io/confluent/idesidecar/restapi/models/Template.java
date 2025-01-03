package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.cloud.scaffold.v1.model.ScaffoldV1Template;
import io.confluent.cloud.scaffold.v1.model.ScaffoldV1TemplateListDataInner;
import io.confluent.cloud.scaffold.v1.model.ScaffoldV1TemplateSpec;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "api_version",
    "kind",
    "id",
    "metadata"
})
@RegisterForReflection
public class Template extends BaseModel<ScaffoldV1TemplateSpec, ObjectMetadata> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @JsonCreator
  public Template(
      @JsonProperty(value = "spec", required = true) ScaffoldV1TemplateSpec templateManifest,
      @JsonProperty(value = "metadata", required = true) ObjectMetadata metadata
  ) {
    super(templateManifest.getName(), metadata, templateManifest);
  }

  public Template(ScaffoldV1TemplateListDataInner item) {
    this(
        OBJECT_MAPPER.convertValue(item.getSpec(), ScaffoldV1TemplateSpec.class),
        new ObjectMetadata(
            item.getMetadata().getSelf().toString(),
            item.getMetadata().getResourceName().toString()
        ));
  }

  public Template(ScaffoldV1Template item) {
    this(
        OBJECT_MAPPER.convertValue(item.getSpec(), ScaffoldV1TemplateSpec.class),
        new ObjectMetadata(
            item.getMetadata().getSelf().toString(),
            item.getMetadata().getResourceName().toString()
        ));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Template that = (Template) o;
    return Objects.equals(metadata, that.metadata)
           && Objects.equals(spec, that.spec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spec, metadata);
  }

  @Override
  public String toString() {
    return "Template{" + "id='" + id + '\'' + ", metadata=" + metadata
           + ", spec=" + spec + '}';
  }
}
