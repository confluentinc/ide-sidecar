package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterInfoProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterStrategyProcessor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@ApplicationScoped
public class ProxyProcessorBeanProducers {

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  WebClientFactory webClientFactory;

  @Produces
  @Singleton
  @Named("clusterProxyProcessor")
  public Processor<ClusterProxyContext, Future<ClusterProxyContext>> clusterProxyProcessor(
      ClusterProxyProcessor clusterProxyProcessor,
      ClusterStrategyProcessor clusterStrategyProcessor,
      ClusterInfoProcessor clusterInfoProcessor,
      ClusterAuthenticationProcessor clusterAuthenticationProcessor
  ) {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        clusterAuthenticationProcessor,
        clusterInfoProcessor,
        clusterStrategyProcessor,
        clusterProxyProcessor,
        new ProxyRequestProcessor<>(webClientFactory),
        new EmptyProcessor<>()
    );
  }
}
