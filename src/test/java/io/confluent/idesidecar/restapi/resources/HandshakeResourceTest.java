package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the handshake route and its 'should only work exactly once' behavior.
 */
@QuarkusTest
public class HandshakeResourceTest {

  /**
   * Same instance as the one used in the handshake route.
   */
  @Inject
  private SidecarAccessTokenBean accessTokenBean;


  /**
   * Reset the bean which the handshake route consults before each test.
   * This is necessary because the bean is a singleton and the handshake route is single-use, and
   * we are otherwise unable to control the lifecycle of the bean. Having each test injection provide
   * a new instance of the bean would have been preferable, but we could not figure out how.
   * 
   * If we do figure out how to control the injection of the bean within the route, then
   * this can be simplified.
   * 
   * This isn't terrible here, because this ugly is encapsulated here in the test.
   */
  @BeforeEach
  void setup() {
    // Make it as if the bean has never generated a token. We do this by setting the token field to null, and via
    // reflection because the field is private and real code should not ever do this.
    try {
      Field tokenField = accessTokenBean.getClass().getDeclaredField("token");
      tokenField.setAccessible(true);
      tokenField.set(accessTokenBean, null);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }


  @Test
  @TestHTTPEndpoint(HandshakeResource.class)
  void shouldReturnHandshakeResponse() throws IOException {

    
    var handshakeToken = get()
        .then().statusCode(200)
        .extract().jsonPath().getString("auth_secret");

    // Should be the same test-scoped bean / token.
    var expectedToken = accessTokenBean.getToken();
    assertEquals(expectedToken, handshakeToken);
  }

  @Test
  @TestHTTPEndpoint(HandshakeResource.class)
  void handshakeShouldOnlyWorkOnce() throws IOException {
    
    // At onset, no token should have been generated.
    assertNull(accessTokenBean.getToken());

    // First time hit should work. Will have called accessTokenBean.generateToken().
    get().then().statusCode(200); 

    assertNotNull(accessTokenBean.getToken());

    // Second time hit should not work. The second call to generateToken() should
    // throw an exception.
    get().then().statusCode(401);

    // When hit subsequent times / 401'd, the responses should include a header with
    // the PID of the sidecar.
    var sidecarPid = get().then().statusCode(401)
        .extract().header("x-sidecar-pid");

    assertEquals(String.valueOf(ProcessHandle.current().pid()), sidecarPid);
  }


}
