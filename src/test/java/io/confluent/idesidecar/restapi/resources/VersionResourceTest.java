package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.get;
import static org.hamcrest.CoreMatchers.is;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@TestProfile(NoAccessFilterProfile.class)
@TestHTTPEndpoint(VersionResource.class)
@QuarkusTest
public class VersionResourceTest {

  @Test
  void testVersion() {
    get()
        .then().statusCode(200)
         // VERSION should be injected by Quarkus with good values in real builds.
        .body("version", is(VersionResource.VERSION));
  }
}
