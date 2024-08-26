package io.confluent.idesidecar.restapi.processors;

/**
 * A generic processor that can be used to chain multiple processors together. Each processor can
 * modify the context and pass it to the next processor.
 *
 * @param <T> The input context type for the processor
 * @param <U> The output context type for the processor
 */
public abstract class Processor<T, U> {

  private Processor<T, U> next;

  public abstract U process(T context);

  public void setNext(Processor<T, U> next) {
    this.next = next;
  }

  public Processor<T, U> next() {
    return next;
  }

  @SafeVarargs
  public static <T, U> Processor<T, U> chain(Processor<T, U>... processors) {
    var start = processors[0];

    var current = start;
    for (int i = 1; i < processors.length; i++) {
      current.setNext(processors[i]);
      current = processors[i];
    }

    return start;
  }
}
