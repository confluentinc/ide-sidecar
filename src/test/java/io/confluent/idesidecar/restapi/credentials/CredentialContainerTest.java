package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.Test;

class CredentialContainerTest extends RedactedTestBase<CredentialContainer> {

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/container_with_api_key_and_secret.json",
        "credentials/container_with_api_key_and_secret_redacted.json",
        CredentialContainer.class
    );
    assertSerializeAndDeserialize(
        "credentials/container_with_basic_credentials.json",
        "credentials/container_with_basic_credentials_redacted.json",
        CredentialContainer.class
    );
  }

  @Test
  void shouldFailToDeserializeContainerWithUnknownCredential() {
    assertThrows(
        JsonParseException.class,
        () -> MAPPER.readValue(
            "credentials/container_with_unknown_credential.json",
            CredentialContainer.class
        )
    );
  }
}