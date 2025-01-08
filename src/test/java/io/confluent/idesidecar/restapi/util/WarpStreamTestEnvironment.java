package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.credentials.TLSConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpecKafkaClusterConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.TestProfile;

import java.util.Optional;

/**
 * A {@link TestEnvironment} that starts a local WarpStream container with a Kafka-compatible broker
 * and Confluent Schema Registry API compatible server.
 */
@TestProfile(NoAccessFilterProfile.class)
public class WarpStreamTestEnvironment implements TestEnvironment {

  private final WarpStreamContainer warpStream;

  public WarpStreamTestEnvironment(String tag) {
    warpStream = new WarpStreamContainer(tag);
  }

  public WarpStreamTestEnvironment(SHA256Digest digest) {
    warpStream = new WarpStreamContainer(digest);
  }

  @Override
  public void start() {
    warpStream.start();
  }

  @Override
  public void shutdown() {
    close();
  }

  @Override
  public Optional<ConnectionSpec> localConnectionSpec() {
    return Optional.empty();
  }

  public void close() {
    warpStream.close();
  }

  public Optional<ConnectionSpec> directConnectionSpec() {
    return Optional.of(
        ConnectionSpec.createDirect(
            "warpstream-direct",
            "WarpStream Direct Connection",
            ConnectionSpecKafkaClusterConfigBuilder
                .builder()
                .bootstrapServers("localhost:9092")
                // Disable TLS
                .tlsConfig(TLSConfigBuilder.builder().enabled(false).build())
                .build(),
            ConnectionSpecSchemaRegistryConfigBuilder
                .builder()
                .uri("http://localhost:9094")
                .build()
        ));
  }
}