package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.CCloudApiProcessor;
import io.confluent.idesidecar.restapi.proxy.ClusterProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.ControlPlaneAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.proxy.ProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.RBACProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.ControlPlaneProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterInfoProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterStrategyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ConfluentCloudKafkaRestProduceProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.KafkaClusterInfoProcessor;
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
  CCloudApiProcessor cCloudApiAuthProcessor;

  @Inject
  EmptyProcessor<ClusterProxyContext> emptyProcessorClusterProxyContext;

  @Inject
  ConnectionProcessor<ClusterProxyContext> connectionProcessorClusterProxyContext;

  @Inject
  ControlPlaneAuthenticationProcessor controlPlaneAuthenticationProcessor;

  @Inject
  ClusterCache clusterCache;

  @Produces
  @Singleton
  @Named("clusterProxyProcessor")
  public Processor<ClusterProxyContext, Future<ClusterProxyContext>> clusterProxyProcessor(
      ClusterProxyProcessor clusterProxyProcessor,
      ClusterStrategyProcessor clusterStrategyProcessor,
      ClusterInfoProcessor clusterInfoProcessor,
      ClusterProxyRequestProcessor clusterProxyRequestProcessor
  ) {
    return Processor.chain(
        connectionProcessorClusterProxyContext,
        new ClusterAuthenticationProcessor<>(),
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
        rbacProxyProcessor,
        controlPlaneAuthenticationProcessor,
        new ProxyRequestProcessor(webClientFactory, vertx),
        emptyProcessorProxyContext
    );
  }

  @Produces
  @Singleton
  @Named("CCloudProxyProcessor")
  public Processor<ProxyContext, Future<ProxyContext>> ccloudProxyProcessor(
      ControlPlaneProxyProcessor genericProxyProcessor
  ) {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        genericProxyProcessor,
        cCloudApiAuthProcessor,
        new ProxyRequestProcessor(webClientFactory, vertx),
        emptyProcessorProxyContext
    );
  }

  @Produces
  @Singleton
  @Named("ccloudProduceProcessor")
  public Processor<KafkaRestProxyContext<ProduceRequest, ProduceResponse>,
      Future<KafkaRestProxyContext<ProduceRequest, ProduceResponse>>> ccloudProduceProcessor(
      ConfluentCloudKafkaRestProduceProcessor confluentCloudKafkaRestProduceProcessor
  ) {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        new ClusterAuthenticationProcessor<>(),
        new KafkaClusterInfoProcessor<>(clusterCache),
        confluentCloudKafkaRestProduceProcessor,
        new EmptyProcessor<>()
    );
  }
}
