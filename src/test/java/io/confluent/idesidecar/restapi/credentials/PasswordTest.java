package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PasswordTest extends RedactedTestBase<Password> {

  @Test
  void shouldMaskToString() {
    var credential = new Password("password".toCharArray());
    assertEquals(Redactable.MASKED_VALUE, credential.toString());
  }

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/credential.json",
        "credentials/credential_redacted.json",
        Password.class
    );
  }

  @Test
  void shouldMaskCredential() {
    var credential = new Password("password".toCharArray());
    assertEquals("********", credential.toString());
  }
}