package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CloudProviderTest {

  @Test
  public void shouldParseValidValues() {
    assertParseProvider(
        CloudProvider.AWS,
        "AWS",
        "Aws",
        "aws",
        " aws",
        " aws  "
    );
    assertParseProvider(
        CloudProvider.GCP,
        "GCP",
        "Gcp",
        "gcp",
        " gcp",
        " gcp  "
    );
    assertParseProvider(
        CloudProvider.AZURE,
        "AZURE",
        "Azure",
        "azure",
        " azure"
    );
    assertParseProvider(
        CloudProvider.NONE,
        null,
        "none",
        "other",
        "  ",
        " \n"
    );
  }

  protected void assertParseProvider(CloudProvider expected, String...values) {
    Stream.of(values).forEach(value -> {
        assertSame(expected, CloudProvider.of(value));
    });
  }

}