package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.*;

import io.vertx.core.MultiMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SanitizeHeadersUtilTest {

  @Test
  void shouldSanitizeHeadersWithExclusions() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Content-Type", "application/json");
    headers.add("Authorization", "Bearer token123");
    headers.add("User-Agent", "Test-Agent");

    List<String> exclusions = List.of("Authorization");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, exclusions);

    // Verify
    assertEquals(2, result.size());
    assertEquals("application/json", result.get("Content-Type"));
    assertEquals("Test-Agent", result.get("User-Agent"));
    assertFalse(result.containsKey("Authorization"));
  }

  @Test
  void shouldReturnAllHeadersWhenNoExclusions() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Content-Type", "application/json");
    headers.add("Authorization", "Bearer token123");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, null);

    // Verify
    assertEquals(2, result.size());
    assertEquals("application/json", result.get("Content-Type"));
    assertEquals("Bearer token123", result.get("Authorization"));
  }

  @Test
  void shouldReturnEmptyMapForEmptyHeaders() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    List<String> exclusions = List.of("Authorization");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, exclusions);

    // Verify
    assertTrue(result.isEmpty());
  }

  @Test
  void shouldHandleEmptyExclusionsList() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Content-Type", "application/json");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, List.of());

    // Verify
    assertEquals(1, result.size());
    assertEquals("application/json", result.get("Content-Type"));
  }

  @Test
  void shouldHandleCaseInsensitiveHeaders() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("content-type", "application/json");
    headers.add("Authorization", "Bearer token123");

    List<String> exclusions = List.of("authorization");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, exclusions);

    // Verify
    assertEquals(1, result.size());
    assertEquals("application/json", result.get("content-type"));
    assertFalse(result.containsKey("Authorization"));
  }

  @Test
  void shouldThrowExceptionForNullHeaders() {
    // Setup
    List<String> exclusions = List.of("Authorization");

    // Execute & Verify
    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> SanitizeHeadersUtil.sanitizeHeaders(null, exclusions)
    );

    assertEquals("Request headers cannot be null", exception.getMessage());
  }

  @Test
  void shouldIgnoreNonExistentExclusionHeaders() {
    // Setup
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("Content-Type", "application/json");

    List<String> exclusions = List.of("NonExistentHeader");

    // Execute
    Map<String, String> result = SanitizeHeadersUtil.sanitizeHeaders(headers, exclusions);

    // Verify
    assertEquals(1, result.size());
    assertEquals("application/json", result.get("Content-Type"));
  }
}