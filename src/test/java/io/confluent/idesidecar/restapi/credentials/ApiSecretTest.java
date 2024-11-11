package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ApiSecretTest extends RedactedTestBase<ApiSecret> {

  @Test
  void shouldMaskToString() {
    var apiSecret = new ApiSecret("secret".toCharArray());
    assertEquals(Redactable.MASKED_VALUE, apiSecret.toString());
  }

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/api_secret.json",
        "credentials/api_secret_redacted.json",
        ApiSecret.class
    );
  }

  @Test
  void shouldMaskApiSecret() {
    var apiSecret = new ApiSecret("secret".toCharArray());
    assertEquals(Redactable.MASKED_VALUE, apiSecret.toString());
  }
}