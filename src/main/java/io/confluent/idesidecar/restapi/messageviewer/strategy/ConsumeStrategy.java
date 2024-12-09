package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.vertx.core.Future;

/**
 * Defines common methods of consume strategies for the message viewer API.
 */
public interface ConsumeStrategy {

  Future<MessageViewerContext> execute(MessageViewerContext context);
}
