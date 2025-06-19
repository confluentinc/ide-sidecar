package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.vertx.core.Future;

/**
 * Chooses the appropriate {@link ConsumeStrategy} depending on the type of the provided Sidecar
 * connection.
 */
public class ConsumeStrategyProcessor
    extends Processor<KafkaRestProxyContext
    <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>,
    Future<KafkaRestProxyContext
        <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>>> {

  private final NativeConsumeStrategy nativeConsumeStrategy;
  private final ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy;

  public ConsumeStrategyProcessor(
      NativeConsumeStrategy nativeConsumeStrategy,
      ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy) {
    this.nativeConsumeStrategy = nativeConsumeStrategy;
    this.confluentCloudConsumeStrategy = confluentCloudConsumeStrategy;
  }

  @Override
  public Future<KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>> process(
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context) {
    var messageConsumer = chooseStrategy(context);
    return messageConsumer.execute(context).compose(updatedContext -> next().process(context));
  }

  public ConsumeStrategy chooseStrategy(KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context) {
    var connectionType = context.getConnectionState().getType();
    return switch (connectionType) {
      case DIRECT, LOCAL -> nativeConsumeStrategy;
      case CCLOUD -> confluentCloudConsumeStrategy;
    };
  }
}
