package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.vertx.core.Future;

/**
 * Defines common methods of consume strategies for the message viewer API.
 */
public interface ConsumeStrategy {
  Future<KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>>
  execute(KafkaRestProxyContext
              <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context);
}
