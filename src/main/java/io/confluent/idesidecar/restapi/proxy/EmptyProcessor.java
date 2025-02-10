package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import jakarta.enterprise.context.Dependent;

/**
 * No-op processor that is typically called at the end of a processing chain.
 */
@Dependent
public class EmptyProcessor<T> extends Processor<T, Future<T>> {

  @Override
  public Future<T> process(T context) {
    // Reached the depth of the request processing chain
    // Now start popping frames off the stack, letting each processor
    // do something with the context on the way out
    return Future.succeededFuture(context);
  }
}
