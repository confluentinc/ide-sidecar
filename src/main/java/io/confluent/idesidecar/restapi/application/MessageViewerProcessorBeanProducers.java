package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.KafkaClusterInfoProcessor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.messageviewer.strategy.ConfluentCloudConsumeStrategy;
import io.confluent.idesidecar.restapi.messageviewer.strategy.ConsumeStrategyProcessor;
import io.confluent.idesidecar.restapi.messageviewer.strategy.NativeConsumeStrategy;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;

/**
 * Sets up the Reactive Routes processing chain for the message viewer API.
 */
@ApplicationScoped
public class MessageViewerProcessorBeanProducers {

  @Inject
  ConnectionStateManager connectionManager;

  @Inject
  NativeConsumeStrategy nativeConsumeStrategy;

  @Inject
  ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy;

  @Inject
  ClusterCache clusterCache;

  @Produces
  @Named("messageViewerProcessor")
  public Processor<KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>,
      Future<KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>>
      > messageViewerCCloudProcessor() {
    return Processor.chain(
        new ConnectionProcessor<>(connectionManager),
        new KafkaClusterInfoProcessor(clusterCache),
        new ConsumeStrategyProcessor(
            nativeConsumeStrategy,
            confluentCloudConsumeStrategy
        ),
        new EmptyProcessor<>()
    );
  }
}
