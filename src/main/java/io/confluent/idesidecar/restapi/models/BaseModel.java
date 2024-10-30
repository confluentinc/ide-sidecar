package io.confluent.idesidecar.restapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.microprofile.config.ConfigProvider;

public abstract class BaseModel<SpecT, MetadataT extends ObjectMetadata> {

  protected static final String API_VERSION = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.api.groupWithVersion", String.class);

  protected final String id;
  protected final MetadataT metadata;
  protected final SpecT spec;

  protected BaseModel(
      String id,
      MetadataT metadata,
      SpecT spec
  ) {
    this.id = id;
    this.metadata = metadata;
    this.spec = spec;
  }

  @JsonProperty(value = "api_version", required = true)
  public String apiVersion() {
    return API_VERSION;
  }

  @JsonProperty(value = "kind", required = true)
  public String kind() {
    return this.getClass().getSimpleName();
  }

  @JsonProperty(value = "id", required = true)
  public String id() {
    return this.id;
  }

  @JsonProperty(value = "spec", required = true)
  public SpecT spec() {
    return this.spec;
  }

  @JsonProperty(value = "metadata", required = true)
  public MetadataT metadata() {
    return this.metadata;
  }
}
