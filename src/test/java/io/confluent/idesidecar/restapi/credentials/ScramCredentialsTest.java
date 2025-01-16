package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.credentials.Credentials.HashAlgorithm;
import org.junit.jupiter.api.Test;

class ScramCredentialsTest extends RedactedTestBase<ScramCredentials> {

  @Test
  void shouldMaskToString() {
    HashAlgorithm hashAlgorithm = HashAlgorithm.SCRAM_SHA_256;
    var username = "username";
    var password = new Password("password".toCharArray());
    var scramCredentials = new ScramCredentials(hashAlgorithm, username, password);
    assertEquals(username, scramCredentials.username());
    assertEquals(Redactable.MASKED_VALUE, scramCredentials.password().toString());
  }

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/scram_credentials.json",
        "credentials/scram_credentials_redacted.json",
        ScramCredentials.class
    );
  }
}