package io.confluent.idesidecar.restapi.models;

import io.confluent.idesidecar.restapi.resources.TemplateResource;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;

public class Template extends BaseModel<TemplateManifest> {

  private Template() {
  }

  public Template(TemplateManifest templateManifest) {
    this.id = templateManifest.name();
    this.spec = templateManifest;
    this.metadata = new ObjectMetadata(
        TemplateResource.API_RESOURCE_PATH,
        templateManifest.name());
  }
}
