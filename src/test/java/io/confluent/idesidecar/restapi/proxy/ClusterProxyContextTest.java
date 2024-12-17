package io.confluent.idesidecar.restapi.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
class ClusterProxyContextTest {

  @InjectMock
  UuidFactory uuidFactory;

  @BeforeEach
  void setUp() {
    Mockito.when(uuidFactory.getRandomUuid()).thenReturn("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4");
  }

  final ClusterProxyContext createDummyContext() {
    return new ClusterProxyContext(
        "http://localhost:8080",
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Test
  void testCreatingFailureWithErrors() {
    ClusterProxyContext context = createDummyContext();
    context.error("code 1", "title 1")
        // Such fluent, much wow
        .error("code 2", "title 2")
        .error("code 3", "title 3");

    var failure = context.fail(429, "Too many requests");

    assertEquals(3, failure.errors().size());
    assertEquals(429, Integer.parseInt(failure.status()));
    assertEquals("proxy_error", failure.code());
    assertEquals("Too many requests", failure.title());
  }

  @Test
  void testCreatingFailureWithoutErrors() {
    ClusterProxyContext context = createDummyContext();

    var failure = context.fail(429, "Too many requests");

    assertEquals(1, failure.errors().size());
    assertEquals(429, Integer.parseInt(failure.status()));
    assertEquals("proxy_error", failure.code());
    assertEquals("Too many requests", failure.title());
  }

  @Test
  void testFormattedFailureMessage() {
    ClusterProxyContext context = createDummyContext();
    var failure = context.failf(429, "Too many requests: %s %d", "This is a test", 42);

    // Such formatting, much %s
    assertEquals("Too many requests: This is a test 42", failure.title());
  }
}