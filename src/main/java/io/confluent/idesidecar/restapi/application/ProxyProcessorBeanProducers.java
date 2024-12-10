package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ConnectionProcessor;
import io.confluent.idesidecar.restapi.proxy.EmptyProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.SchemaRegistryRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.KafkaClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.SchemaRegistryClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterAuthenticationProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterInfoProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.processors.ClusterStrategyProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentCloudKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentCloudSchemaRegistryClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentLocalKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentLocalSchemaRegistryClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.DirectKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.DirectSchemaRegistryClusterStrategy;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Sets up the Reactive Routes processing chain for proxies, like the Kafka REST Proxy.
 */
@ApplicationScoped
public class ProxyProcessorBeanProducers {

  @Inject
  ClusterCache clusterCache;

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  WebClientFactory webClientFactory;

  @Inject
  ConfluentCloudKafkaClusterStrategy confluentCloudKafkaClusterStrategy;

  @Inject
  ConfluentLocalKafkaClusterStrategy confluentLocalKafkaClusterStrategy;

  @Inject
  DirectKafkaClusterStrategy directKafkaClusterStrategy;

  @Inject
  ConfluentCloudSchemaRegistryClusterStrategy confluentCloudSchemaRegistryClusterStrategy;

  @Inject
  ConfluentLocalSchemaRegistryClusterStrategy confluentLocalSchemaRegistryClusterStrategy;

  @Inject
  DirectSchemaRegistryClusterStrategy directSchemaRegistryClusterStrategy;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;


  @Produces
  @Singleton
  @Named("kafkaClusterProxyProcessor")
  public Processor<KafkaClusterProxyContext, Future<KafkaClusterProxyContext>> clusterProxyProcessor(
  ) {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        new ClusterAuthenticationProcessor<>(),
        new ClusterInfoProcessor<>(clusterCache),
        new ClusterStrategyProcessor<>(
            confluentCloudKafkaClusterStrategy,
            confluentLocalKafkaClusterStrategy,
            directKafkaClusterStrategy,
            confluentCloudSchemaRegistryClusterStrategy,
            confluentLocalSchemaRegistryClusterStrategy,
            directSchemaRegistryClusterStrategy
        ),
        new ClusterProxyProcessor<>(sidecarHost),
        new ProxyRequestProcessor<>(webClientFactory),
        new EmptyProcessor<>()
    );
  }

  @Produces
  @Singleton
  @Named("srClusterProxyProcessor")
  public Processor<SchemaRegistryClusterProxyContext, Future<SchemaRegistryClusterProxyContext>>
  srClusterProxyProcessor() {
    return Processor.chain(
        new ConnectionProcessor<>(connectionStateManager),
        new ClusterAuthenticationProcessor<>(),
        new ClusterInfoProcessor<>(clusterCache),
        new ClusterStrategyProcessor<>(
            confluentCloudKafkaClusterStrategy,
            confluentLocalKafkaClusterStrategy,
            directKafkaClusterStrategy,
            confluentCloudSchemaRegistryClusterStrategy,
            confluentLocalSchemaRegistryClusterStrategy,
            directSchemaRegistryClusterStrategy
        ),
        new ClusterProxyProcessor<>(sidecarHost),
        new SchemaRegistryRequestProcessor<>(),
        new EmptyProcessor<>()
    );
  }
}
