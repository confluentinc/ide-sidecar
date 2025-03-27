package io.confluent.idesidecar.restapi.credentials;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
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

  @Test
  void warpstreamLengthCredentialsShouldPassValidation() {
    // 69 char username, 68 char password from prior examples failing.
    var username = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    var password = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    var creds = new BasicCredentials(username, new Password(password.toCharArray()));

    var errors = new ArrayList<Error>();
    creds.validate(errors, "path", "what");
    assertEquals(0, errors.size());
  }
}