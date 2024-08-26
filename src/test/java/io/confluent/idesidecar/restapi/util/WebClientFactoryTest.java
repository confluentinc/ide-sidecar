package io.confluent.idesidecar.restapi.util;


import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class WebClientFactoryTest {

  @Inject WebClientFactory webClientFactory;

  @Test
  void getWebClientShouldReturnTheSameInstanceWhenCalledMultipleTimes() {
    var firstInstance = webClientFactory.getWebClient();
    var secondInstance = webClientFactory.getWebClient();

    Assertions.assertSame(firstInstance, secondInstance);
  }
}
