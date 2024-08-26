package io.confluent.idesidecar.restapi.util;


import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;
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
    Assertions.assertEquals(2, numberOfAmpersands);
  }

  @Test
  void getEncodeUriShouldEscapeSpecialCharacters() {
    var expectedResult = "dtx%40confluent.io";

    var encodedUri = uriUtil.encodeUri("dtx@confluent.io");

    Assertions.assertEquals(expectedResult, encodedUri);
  }
}
