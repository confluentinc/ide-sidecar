package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.model.AlterConfigBatchRequestDataDataInner;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;

public record AlterConfigCommand(
    String name,
    String value,
    AlterConfigOperation operation
) {

  AlterConfigCommand(String name, String value, String operation) {
    this(name, value, AlterConfigOperation.fromValue(operation));
  }

  AlterConfigOp toAlterConfigOp() {
    return new AlterConfigOp(
        new ConfigEntry(name, value),
        getOpType()
    );
  }

  static AlterConfigCommand fromAlterConfigRequestDataInner(
      AlterConfigBatchRequestDataDataInner inner
  ) {
    return new AlterConfigCommand(inner.getName(), inner.getValue(), inner.getOperation());
  }

  private AlterConfigOp.OpType getOpType() {
    if (operation == AlterConfigOperation.SET) {
      return AlterConfigOp.OpType.SET;
    } else if (operation == AlterConfigOperation.DELETE) {
      return AlterConfigOp.OpType.DELETE;
    }
    throw new UnknownAlterConfigOperation(operation.toString());
  }

  enum AlterConfigOperation {
    SET("SET"),
    DELETE("DELETE");

    private final String value;

    AlterConfigOperation(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }

    public static AlterConfigOperation fromValue(String value) {
      for (AlterConfigOperation b : AlterConfigOperation.values()) {
        if (String.valueOf(b.value).equalsIgnoreCase(value)) {
          return b;
        }
      }
      throw new UnknownAlterConfigOperation(value);
    }
  }
}
