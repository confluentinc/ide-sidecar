package io.confluent.idesidecar.restapi.messageviewer.strategy;

import static org.junit.Assert.assertThrows;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class NativeConsumeStrategyTest {

  @Inject
  NativeConsumeStrategy nativeConsumeStrategy;

  @Test
  void handleKafkaExceptionReturnsMeaningfulMessageForErrorsRelatedToSnappy() {
    var context = new KafkaRestProxyContext<SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>(
        "connectionId", "clusterId", "topicName", null);
    assertThrows(
        ProcessorFailedException.class,
        () -> nativeConsumeStrategy.handleKafkaException(
            new KafkaException(
                "Received exception when fetching the next record from snappy-1. If needed, please"
                    + " seek past the record to continue consumption."
            ),
            context
        )
    );
  }

  @Test
  void handleKafkaExceptionRethrowsExceptionForErrorsNotRelatedToSnappy() {
    var context = new KafkaRestProxyContext<SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>(
        "connectionId", "clusterId", "topicName", null);
    assertThrows(
        KafkaException.class,
        () -> nativeConsumeStrategy.handleKafkaException(
            new KafkaException(
                "Hey, I'm not Snappy!."
            ),
            context
        )
    );
  }
}
