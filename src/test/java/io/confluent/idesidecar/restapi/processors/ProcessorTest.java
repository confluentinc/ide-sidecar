package io.confluent.idesidecar.restapi.processors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;

class ProcessorTest {

  @Test
  void testProcessorChain() throws Exception {
    class CustomContext extends ClusterProxyContext {

      private Integer value;

      public CustomContext(Integer value) {
        super(null, null, null, null, null, null, null, null);
        this.value = value;
      }

      public Integer getValue() {
        return value;
      }

      public void setValue(Integer value) {
        this.value = value;
      }
    }
    Callable<Processor<CustomContext, CustomContext>> createProcessor = () -> new Processor<>() {
      @Override
      public CustomContext process(CustomContext context) {
        context.setValue(context.getValue() + 1);
        return context;
      }
    };

    var emptyProcessor = new Processor<CustomContext, CustomContext>() {
      @Override
      public CustomContext process(CustomContext context) {
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

    var result = processor.process(new CustomContext(0)).getValue();
    // 10 on the way in, 10 on the way out
    assertEquals(20, result);
  }

}