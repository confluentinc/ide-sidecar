package io.confluent.idesidecar.restapi.util;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

public class WarpStreamContainer extends GenericContainer<WarpStreamContainer> {

  // https://gallery.ecr.aws/warpstream-labs/warpstream_agent
  private static final String WARPSTREAM_IMAGE = "public.ecr.aws/warpstream-labs/warpstream_agent";
  private static final Integer KAFKA_PORT = 9092;
  private static final Integer SCHEMA_REGISTRY_PORT = 9094;

  void initialize() {
    super
        .withCreateContainerCmdModifier(
            cmd -> cmd.withCmd("playground")
        );
    super.addFixedExposedPort(KAFKA_PORT, KAFKA_PORT);
    super.addFixedExposedPort(SCHEMA_REGISTRY_PORT, SCHEMA_REGISTRY_PORT);

    super.waitingFor(
        new WaitAllStrategy()
            .withStrategy(
                Wait.forLogMessage(".*You now have WarpStream Kafka.*", 1)
            )
    );
  }

  public WarpStreamContainer(String tag) {
    super(WARPSTREAM_IMAGE + ":" + tag);
    initialize();
  }

  public WarpStreamContainer(SHA256Digest digest) {
    super(WARPSTREAM_IMAGE + "@sha256:" + digest);
    initialize();
  }
}
