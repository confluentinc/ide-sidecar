package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.models.ObjectMetadata.selfFromResourcePath;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.idesidecar.restapi.resources.TemplateResource;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
    "api_version",
    "kind",
    "id",
    "metadata"
})
public class Template extends BaseModel<TemplateManifest, ObjectMetadata> {

  @JsonCreator
  public Template(
      @JsonProperty(value = "spec", required = true) TemplateManifest templateManifest,
      @JsonProperty(value = "metadata", required = true) ObjectMetadata metadata
  ) {
    super(templateManifest.name(), metadata, templateManifest);
  }

  public Template(TemplateManifest templateManifest) {
    this(
        templateManifest,
        new ObjectMetadata(
            selfFromResourcePath(TemplateResource.API_RESOURCE_PATH, templateManifest.name()),
            null
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
