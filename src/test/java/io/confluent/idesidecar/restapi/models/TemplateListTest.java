package io.confluent.idesidecar.restapi.models;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.deserializeAndSerialize;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.serializeAndDeserialize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

class TemplateListTest {

  @Test
  void shouldCreateEmptyListWithMetadata() {
    var templateList = new TemplateList(List.of());
    assertEquals(0, templateList.data().size());

    assertNotNull(templateList.metadata());
    assertNotNull(templateList.metadata().self());
    assertNull(templateList.metadata().next());
    assertEquals(0, templateList.metadata().totalSize());
  }

  @Test
  void shouldSerializeAndDeserializeEmptyList() {
    var templateList = new TemplateList(List.of());
    serializeAndDeserialize(templateList);
  }

  @Test
  void shouldDeserializeAndSerializeNonEmptyList() {
    deserializeAndSerialize(
        "templates-api-responses/list-templates-response.json",
        ConnectionsList.class
    );
  }
}
