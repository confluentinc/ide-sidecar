package io.confluent.idesidecar.restapi.util.cpdemo;

import com.github.dockerjava.api.model.HealthCheck;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import java.util.List;

public class ToolsContainer extends GenericContainer<ToolsContainer> {
  private static final String DEFAULT_IMAGE = "cnfldemos/tools";
  private static final String DEFAULT_TAG = "0.3";
  private static final String CONTAINER_NAME = "tools";

  public ToolsContainer(String tag, Network network) {
    super(DEFAULT_IMAGE + ":" + tag);
    super.withNetwork(network);
    super.withNetworkAliases(CONTAINER_NAME);
    super.withEnv("TZ", "America/New_York");

    super.withFileSystemBind(
        ".cp-demo/scripts/security",
        "/etc/kafka/secrets"
    );
    super.withFileSystemBind(
        ".cp-demo/scripts/helper",
        "/tmp/helper"
    );
    super.withCreateContainerCmdModifier(cmd ->
        cmd
            .withEntrypoint("/bin/bash")
            .withTty(true)
            .withHealthcheck(
                new HealthCheck()
                    .withTest(List.of(
                        "CMD",
                        "bash", "-c", "echo 'health check'"
                    )
            ))
    );
  }

  public ToolsContainer(Network network) {
    this(DEFAULT_TAG, network);
  }
}
