package io.confluent.idesidecar.restapi.credentials;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ApiKeyAndSecretTest extends RedactedTestBase<ApiKeyAndSecret> {

  @Test
  void shouldMaskToString() {
    var key = "api-key";
    var keyAndSecret = new ApiKeyAndSecret(key, new ApiSecret("api-secret".toCharArray()));
    assertEquals(key, keyAndSecret.key());
    assertEquals(Redactable.MASKED_VALUE, keyAndSecret.secret().toString());
  }

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/api_key_and_secret.json",
        "credentials/api_key_and_secret_redacted.json",
        ApiKeyAndSecret.class
    );
  }
}