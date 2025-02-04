package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ClusterProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.ControlPlaneAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.RBACProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterInfoProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterStrategyProcessor;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

/**
 * Sets up the Reactive Routes processing chain for proxies, like the Kafka REST Proxy.
 */
@ApplicationScoped
public class ProxyProcessorBeanProducers {

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  Vertx vertx;

  @Inject
  EmptyProcessor<ProxyContext> emptyProcessorProxyContext;

  @Inject
  EmptyProcessor<ClusterProxyContext> emptyProcessorClusterProxyContext;

  @Inject
  ConnectionProcessor<ClusterProxyContext> connectionProcessorClusterProxyContext;

  @Inject
  ControlPlaneAuthenticationProcessor controlPlaneAuthenticationProcessor;

  @Produces
  @Singleton
  @Named("clusterProxyProcessor")
  public Processor<ClusterProxyContext, Future<ClusterProxyContext>> clusterProxyProcessor(
      ClusterProxyProcessor clusterProxyProcessor,
      ClusterStrategyProcessor clusterStrategyProcessor,
      ClusterInfoProcessor clusterInfoProcessor,
      ClusterAuthenticationProcessor clusterAuthenticationProcessor,
      ClusterProxyRequestProcessor clusterProxyRequestProcessor
  ) {
    return Processor.chain(
        connectionProcessorClusterProxyContext,
        clusterAuthenticationProcessor,
        clusterInfoProcessor,
        clusterStrategyProcessor,
        clusterProxyProcessor,
        clusterProxyRequestProcessor,
        emptyProcessorClusterProxyContext
    );

  }

  @Produces
  @Singleton
  @Named("RBACProxyProcessor")
  public Processor<ProxyContext, Future<ProxyContext>> RbacProxyProcessor(
      RBACProxyProcessor rbacProxyProcessor
  ) {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        controlPlaneAuthenticationProcessor,
        rbacProxyProcessor,
        new ProxyRequestProcessor(webClientFactory, vertx),
        emptyProcessorProxyContext
    );
  }
}