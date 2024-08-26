package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.vertx.core.Future;

public interface ConsumeStrategy {
  Future<MessageViewerContext> execute(MessageViewerContext context);
}
