package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;

public class ConsumeStrategyProcessor
    extends Processor<MessageViewerContext, Future<MessageViewerContext>> {
  private final ConfluentLocalConsumeStrategy confluentLocalConsumeStrategy;
  private final ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy;

  public ConsumeStrategyProcessor(
      ConfluentLocalConsumeStrategy confluentLocalConsumeStrategy,
      ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy) {
    this.confluentLocalConsumeStrategy = confluentLocalConsumeStrategy;
    this.confluentCloudConsumeStrategy = confluentCloudConsumeStrategy;
  }

  @Override
  public Future<MessageViewerContext> process(MessageViewerContext context) {
    var messageConsumer = chooseStrategy(context);
    return messageConsumer.execute(context).compose(updatedContext -> {
      return next().process(context);
    });
  }

  public ConsumeStrategy chooseStrategy(MessageViewerContext context) {
    var connectionType = context.getConnectionState().getType();
    return switch (connectionType) {
      case LOCAL -> confluentLocalConsumeStrategy;
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
