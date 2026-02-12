package io.confluent.idesidecar.restapi.credentials;

import static org.apache.kafka.common.config.internals.BrokerSecurityConfigs.ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OAuthCredentialsTest {

  private OAuthCredentials credentials;

  @BeforeEach
  void setUp() {
    credentials = new OAuthCredentials(
        "https://example.com/token",
        "client-id",
        new Password("secret".toCharArray())
    );
    // Clear the system property before each test
    System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
  }

  @AfterEach
  void tearDown() {
    // Clean up after each test
    System.clearProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG);
  }

  @Test
  void addTokensUrlToAllowedUrls_shouldSetPropertyWhenNotSet() {
    // Given: system property is not set
    assertNull(System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG));

    // When: addTokensUrlToAllowedUrls is called
    credentials.addTokensUrlToAllowedUrls("https://example.com/token");

    // Then: system property should be set to the token URL
    assertEquals(
        "https://example.com/token",
        System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
    );
  }

  @Test
  void addTokensUrlToAllowedUrls_shouldNotModifyPropertyWhenUrlAlreadyPresent() {
    // Given: system property already contains the URL
    System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "https://example.com/token");

    // When: addTokensUrlToAllowedUrls is called with the same URL
    credentials.addTokensUrlToAllowedUrls("https://example.com/token");

    // Then: system property should remain unchanged
    assertEquals(
        "https://example.com/token",
        System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
    );
  }

  @Test
  void addTokensUrlToAllowedUrls_shouldAppendUrlWhenDifferentUrlPresent() {
    // Given: system property contains a different URL
    System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, "https://other.com/token");

    // When: addTokensUrlToAllowedUrls is called with a new URL
    credentials.addTokensUrlToAllowedUrls("https://example.com/token");

    // Then: system property should contain both URLs
    assertEquals(
        "https://other.com/token,https://example.com/token",
        System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
    );
  }

  @Test
  void addTokensUrlToAllowedUrls_shouldNotDuplicateUrlInList() {
    // Given: system property contains multiple URLs including the one we want to add
    System.setProperty(
        ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
        "https://first.com/token,https://example.com/token"
    );

    // When: addTokensUrlToAllowedUrls is called with a URL already in the list
    credentials.addTokensUrlToAllowedUrls("https://example.com/token");

    // Then: system property should remain unchanged
    assertEquals(
        "https://first.com/token,https://example.com/token",
        System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG)
    );
  }
}
