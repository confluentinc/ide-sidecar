package io.confluent.idesidecar.restapi.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class ProcessorTest {

  @Test
  void testProcessorChain() throws Exception {
    Callable<Processor<Integer, Integer>> createProcessor = () -> new Processor<>() {
      @Override
      public Integer process(Integer context) {
        context = context + 1;
        context = next().process(context);
        context = context + 1;
        return context;
      }
    };

    var emptyProcessor = new Processor<Integer, Integer>() {
      @Override
      public Integer process(Integer context) {
        return context;
      }
    };

    var processor = Processor.chain(
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        createProcessor.call(),
        emptyProcessor
    );

    var result = processor.process(0);
    // 10 on the way in, 10 on the way out
    assertEquals(20, result);
  }

}