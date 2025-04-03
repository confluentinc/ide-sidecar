package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class UriUtilTest {

  @Inject
  UriUtil uriUtil;

  @Test
  void getFlattenedQueryParametersShouldFlattenedAndEscapedParameters() {
    var queryParameters = new MultivaluedHashMap<String, String>();
    queryParameters.put("email", List.of("dtx@confluent.io"));
    queryParameters.put("scope[]", List.of("email", "offline"));
    UriInfo uriInfo = Mockito.mock(UriInfo.class);
    Mockito.when(uriInfo.getQueryParameters()).thenReturn(queryParameters);

    var flattenedQueryParameters = uriUtil.getFlattenedQueryParameters(uriInfo);

    Assertions.assertTrue(flattenedQueryParameters.startsWith("?"));
    // The order of the query parameters is non-deterministic, so we can't use
    // Assertions.assertEquals(expected, actual)
    Assertions.assertTrue(flattenedQueryParameters.contains("scope[]=email"));
    Assertions.assertTrue(flattenedQueryParameters.contains("scope[]=offline"));
    Assertions.assertTrue(flattenedQueryParameters.contains("email=dtx%40confluent.io"));
    // We expect two joining ampersands because we have three query parameters
    var numberOfAmpersands = flattenedQueryParameters.chars()
        .filter(character -> character == '&').count();
    assertEquals(2, numberOfAmpersands);
  }

  @Test
  void getEncodeUriShouldEscapeSpecialCharacters() {
    var expectedResult = "dtx%40confluent.io";

    var encodedUri = uriUtil.encodeUri("dtx@confluent.io");

    assertEquals(expectedResult, encodedUri);
  }

  @Test
  void shouldReturnHostnameAndPortForUriWithoutPort() throws URISyntaxException {
    var uri = "http://localhost";
    var expected = "localhost";
    assertEquals(expected, uriUtil.getHostAndPort(uri));
    assertEquals(expected, uriUtil.getHostAndPort(new URI(uri)));
  }

  @Test
  void shouldReturnHostnameAndPortForUriWithoutPortButWithDefaultPort() throws URISyntaxException {
    var uri = "http://localhost:80";
    var expected = "localhost:80";
    assertEquals(expected, uriUtil.getHostAndPort(uri, 443));
    assertEquals(expected, uriUtil.getHostAndPort(new URI(uri), 443));

    uri = "http://localhost";
    assertEquals(expected, uriUtil.getHostAndPort(uri, 80));
    assertEquals(expected, uriUtil.getHostAndPort(new URI(uri), 80));
  }

  @Test
  void shouldReturnHostnameAndPortForUriWithPort() throws URISyntaxException {
    var uri = "http://localhost:8082";
    var expected = "localhost:8082";
    assertEquals(expected, uriUtil.getHostAndPort(uri));
    assertEquals(expected, uriUtil.getHostAndPort(new URI(uri)));
  }

  @Test
  void testCombineWithoutLeadingSlashInRelativePath() {
    String baseUrl = "https://flink.us-west-2.aws.confluent.cloud";
    String relativePath = "test";

    String result = uriUtil.combine(baseUrl, relativePath);

    assertEquals("https://flink.us-west-2.aws.confluent.cloud/test", result);
  }

  @Test
  void testCombineWithLeadingSlashInRelativePath() {
    String baseUrl = "https://flink.us-west-2.aws.confluent.cloud";
    String relativePath = "/test";

    String result = uriUtil.combine(baseUrl, relativePath);

    assertEquals("https://flink.us-west-2.aws.confluent.cloud/test", result);
  }

  @Test
  void testCombineWithTrailingSlashInBaseUrl() {
    String baseUrl = "https://flink.us-west-2.aws.confluent.cloud/";
    String relativePath = "test";

    String result = uriUtil.combine(baseUrl, relativePath);

    assertEquals("https://flink.us-west-2.aws.confluent.cloud/test", result);
  }

  @Test
  void testCombineWithBothSlashes() {
    String baseUrl = "https://flink.us-west-2.aws.confluent.cloud";
    String relativePath = "/test";

    String result = uriUtil.combine(baseUrl, relativePath);

    assertEquals("https://flink.us-west-2.aws.confluent.cloud/test", result);
  }

  @Test
  void testCombineWithComplexPaths() {
    String baseUrl = "https://api.confluent.cloud/something";
    String relativePath = "/v1/resources";

    String result = uriUtil.combine(baseUrl, relativePath);

    assertEquals("https://api.confluent.cloud/something/v1/resources", result);
  }
}
