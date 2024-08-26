package io.confluent.idesidecar.restapi.models;

import io.confluent.idesidecar.restapi.resources.TemplateResource;
import java.util.List;

public class TemplateList extends BaseList<Template> {

  private TemplateList() {
  }

  public TemplateList(List<Template> templateList) {
    this.metadata = new CollectionMetadata(
        templateList.size(),
        TemplateResource.API_RESOURCE_PATH);
    this.data = templateList;
  }
}
