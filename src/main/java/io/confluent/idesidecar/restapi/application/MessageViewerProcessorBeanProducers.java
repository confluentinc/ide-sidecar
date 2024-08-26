package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerClusterInfoProcessor;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.strategy.ConfluentCloudConsumeStrategy;
import io.confluent.idesidecar.restapi.messageviewer.strategy.ConfluentLocalConsumeStrategy;
import io.confluent.idesidecar.restapi.messageviewer.strategy.ConsumeStrategyProcessor;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;


@ApplicationScoped
public class MessageViewerProcessorBeanProducers {

  @Inject
  ConnectionStateManager connectionManager;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Inject
  ConfluentLocalConsumeStrategy confluentLocalConsumeStrategy;

  @Inject
  ConfluentCloudConsumeStrategy confluentCloudConsumeStrategy;

  @Inject
  ClusterCache clusterCache;

  @Produces
  @Named("messageViewerProcessor")
  public Processor<MessageViewerContext,
          Future<MessageViewerContext>> messageViewerCCloudProcessor() {
    return Processor.chain(
        new ConnectionProcessor<>(connectionManager),
        new MessageViewerClusterInfoProcessor(clusterCache),
        new ConsumeStrategyProcessor(
            confluentLocalConsumeStrategy,
            confluentCloudConsumeStrategy
        ),
        new EmptyProcessor<>()
    );
  }
}
