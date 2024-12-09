package io.confluent.idesidecar.restapi.credentials;

import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
  void shouldGetHttpClientHeaders() {
    var username = "ABCDEFGH12345678";
    var basicCreds = new BasicCredentials(username, new Password(
        "K1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0".toCharArray()));
    var headers = basicCreds.httpClientHeaders();
    var authValue = headers.orElseThrow().get(AUTHORIZATION);
    assertEquals(
        "Basic QUJDREVGR0gxMjM0NTY3ODpLMTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVow",
        authValue
    );
  }
}