package io.confluent.idesidecar.restapi.application;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.confluent.idesidecar.restapi.exceptions.TokenAlreadyGeneratedException;
import org.junit.jupiter.api.Test;

public class SidecarAccessTokenBeanTest {

  @Test
  void testGetToken() throws TokenAlreadyGeneratedException {
    var accessTokenBean = new SidecarAccessTokenBean();

    // Generate returns a new random token ...
    var generated = accessTokenBean.generateToken();
    assertEquals(SidecarAccessTokenBean.TOKEN_LENGTH, generated.length());

    // and getToken() returns the same token.
    var token = accessTokenBean.getToken();
    assertEquals(generated, token);

    // ad nauseam
    var secondToken = accessTokenBean.getToken();
    assertEquals(token, secondToken);
      
    // Second instance should generate a different token
    var secondInstance = new SidecarAccessTokenBean();
    assertNotEquals(token, secondInstance.generateToken());
  }

  @Test
  void testGenerateExactlyOnce() {
    // Works the first time
    var accessTokenBean = new SidecarAccessTokenBean();

    // First time should not throw
    assertDoesNotThrow(() -> {
      accessTokenBean.generateToken();
    });
    
    
    // Subsequent times must throw so that /handshake route can return 401.
    assertThrows(TokenAlreadyGeneratedException.class, () -> {
      accessTokenBean.generateToken();
    });
  }

}
