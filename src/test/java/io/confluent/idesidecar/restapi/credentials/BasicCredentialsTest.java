package io.confluent.idesidecar.restapi.credentials;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.credentials.Credentials.Type;
import org.junit.jupiter.api.Test;

class BasicCredentialsTest extends RedactedTestBase<BasicCredentials> {

  @Test
  void shouldMaskToString() {
    var username = "j.acme";
    var creds = new BasicCredentials(username, new Password("my-secret".toCharArray()));
    assertEquals(username, creds.username());
    assertEquals(Redactable.MASKED_VALUE, creds.password().toString());
  }

  @Test
  void shouldSerializeAndDeserialize() {
    assertSerializeAndDeserialize(
        "credentials/basic_credentials.json",
        "credentials/basic_credentials_redacted.json",
        BasicCredentials.class
    );
  }
}