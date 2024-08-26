package io.confluent.idesidecar.scaffolding;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.scaffolding.LocalTemplateRegistryService;
import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryException;
import org.junit.jupiter.api.Test;

class LocalTemplateRegistryServiceTest {

  @Test
  void testEmptyRegistryService() {
    var emptyRegistryService = new LocalTemplateRegistryService(null);

    // We expect an exception to be thrown because the registry directory is not set.
    assertThrows(TemplateRegistryException.class, emptyRegistryService::onStartup);

    // But the registry service should still be created, and can be queried for templates.
    assertTrue(emptyRegistryService.listTemplates().isEmpty());

    assertThrows(TemplateRegistryException.class, () ->
        emptyRegistryService.getTemplate("foo")
    );
  }

}