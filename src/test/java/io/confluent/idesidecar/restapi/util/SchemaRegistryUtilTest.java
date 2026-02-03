package io.confluent.idesidecar.restapi.util;

import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class SchemaRegistryUtilTest {

    @Test
    void shouldRemoveOnlyOAuthPrefixedKeys() {
      var input = new HashMap<String, Object>();
      input.put("bearer.auth.token.endpoint.url", "https://foo");
      input.put("bearer.auth.client.id", "bar");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertTrue(result.isEmpty());
    }

    @Test
    void shouldLeaveNonOAuthKeysUntouched() {
      var input = new HashMap<String, Object>();
      input.put("schema.registry.url", "http://localhost:8081");
      input.put("basic.auth.user.info", "user:pass");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertEquals(input, result);
    }

    @Test
    void shouldRemoveOnlyOAuthKeysAndLeaveOthersUntouched() {
      var input = new HashMap<String, Object>();
      input.put("bearer.auth.token.endpoint.url", "https://foo");
      input.put("schema.registry.url", "http://localhost:8081");
      input.put("bearer.auth.client.id", "bar");
      input.put("basic.auth.user.info", "user:pass");
      var expected = new HashMap<String, Object>();
      expected.put("schema.registry.url", "http://localhost:8081");
      expected.put("basic.auth.user.info", "user:pass");
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertEquals(expected, result);
    }

    @Test
    void shouldHandleEmptyMap() {
      var input = new HashMap<String, Object>();
      var result = SchemaRegistryUtil.removeOAuthConfigs(input);
      assertTrue(result.isEmpty());
    }

    @Test
    void shouldThrowNullPointerExceptionOnNullInput() {
      assertThrows(
          NullPointerException.class,
          () -> SchemaRegistryUtil.removeOAuthConfigs(null)
      );
    }
}
