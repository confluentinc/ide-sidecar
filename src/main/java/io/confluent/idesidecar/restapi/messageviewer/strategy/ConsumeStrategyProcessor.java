package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;

/**
 * Chooses the appropriate {@link ConsumeStrategy} depending on the type of the provided Sidecar
 * connection.
 */
public class ConsumeStrategyProcessor
    extends Processor<MessageViewerContext, Future<MessageViewerContext>> {

  private final NativeConsumeStrategy nativeConsumeStrategy;
  private final ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy;

  public ConsumeStrategyProcessor(
      NativeConsumeStrategy nativeConsumeStrategy,
      ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy) {
    this.nativeConsumeStrategy = nativeConsumeStrategy;
    this.confluentCloudConsumeStrategy = confluentCloudConsumeStrategy;
  }

  @Override
  public Future<MessageViewerContext> process(MessageViewerContext context) {
    var messageConsumer = chooseStrategy(context);
    return messageConsumer.execute(context).compose(updatedContext -> next().process(context));
  }

  public ConsumeStrategy chooseStrategy(MessageViewerContext context) {
    var connectionType = context.getConnectionState().getType();
    return switch (connectionType) {
      case DIRECT, LOCAL -> nativeConsumeStrategy;
      case CCLOUD -> confluentCloudConsumeStrategy;
      case PLATFORM -> throw new ProcessorFailedException(
          context.failf(
              501,
              "This endpoint does not yet support connection-type=%s",
              connectionType
          )
      );
    };
  }
}
