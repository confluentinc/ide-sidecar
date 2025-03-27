package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import java.util.ArrayList;
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

  @Test
  void warpstreamLengthCredentialsShouldPassValidation() {
    // 69 char key, 68 char secret from prior examples failing.
    var key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    var secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    var creds = new ApiKeyAndSecret(key, new ApiSecret(secret.toCharArray()));

    var errors = new ArrayList<Error>();
    creds.validate(errors, "path", "what");
    assertEquals(0, errors.size());
  }
}