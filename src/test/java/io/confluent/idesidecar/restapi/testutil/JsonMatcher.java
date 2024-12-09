package io.confluent.idesidecar.restapi.testutil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class JsonMatcher extends TypeSafeMatcher<String> {

  private final String expectedJson;

  public JsonMatcher(String expectedJson) {
    this.expectedJson = expectedJson;
  }

  @Override
  protected boolean matchesSafely(String actualJson) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      JsonNode expectedNode = objectMapper.readTree(expectedJson);
      JsonNode actualNode = objectMapper.readTree(actualJson);
      return expectedNode.equals(actualNode);
    } catch (Exception e) {
      return false;  // In case of any exception, return false (mismatch)
    }
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("Expected JSON to match: " + expectedJson);
  }

  @Override
  protected void describeMismatchSafely(String item, Description mismatchDescription) {
    mismatchDescription.appendText("was " + item);
  }

  // Factory method to make the matcher easier to use
  public static JsonMatcher matchesJson(String expectedJson) {
    return new JsonMatcher(expectedJson);
  }
}
