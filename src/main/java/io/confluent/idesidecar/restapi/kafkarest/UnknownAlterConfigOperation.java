package io.confluent.idesidecar.restapi.kafkarest;

import java.util.Arrays;

public class UnknownAlterConfigOperation extends IllegalArgumentException {
  public UnknownAlterConfigOperation(String name, String operation) {
    super("Invalid operation %s for configuration %s: must be one of: %s"
        .formatted(
            operation,
            name,
            Arrays.toString(AlterConfigCommand.AlterConfigOperation.values())
        )
    );
  }
}
