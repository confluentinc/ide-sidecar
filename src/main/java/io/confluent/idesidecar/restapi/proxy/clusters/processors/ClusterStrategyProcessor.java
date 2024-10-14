package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentCloudKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentCloudSchemaRegistryClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentLocalKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ConfluentLocalSchemaRegistryClusterStrategy;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * This processor is responsible for choosing the correct strategy for the given cluster type and
 * connection type.
 */
@ApplicationScoped
public class ClusterStrategyProcessor extends
    Processor<ClusterProxyContext, Future<ClusterProxyContext>> {
  @Inject
  ConfluentCloudKafkaClusterStrategy confluentCloudKafkaClusterStrategy;

  @Inject
  ConfluentLocalKafkaClusterStrategy confluentLocalKafkaClusterStrategy;

  @Inject
  ConfluentCloudSchemaRegistryClusterStrategy confluentCloudSchemaRegistryClusterStrategy;

  @Inject
  ConfluentLocalSchemaRegistryClusterStrategy confluentLocalSchemaRegistryClusterStrategy;


  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    var connectionType = context.getConnectionState().getType();
    var clusterType = context.getClusterType();

    var strategy = chooseStrategy(clusterType, connectionType);
    if (strategy == null) {
      return Future.failedFuture(
          new ProcessorFailedException(context.failf(501, "Cannot handle request")));
    } else {
      context.setClusterStrategy(strategy);
      return next().process(context);
    }
  }

  public ClusterStrategy chooseStrategy(
      ClusterType clusterType, ConnectionType connectionType) {
    return switch (connectionType) {
      case CCLOUD -> clusterType == ClusterType.KAFKA
          ? confluentCloudKafkaClusterStrategy : confluentCloudSchemaRegistryClusterStrategy;
      case LOCAL ->
          clusterType == ClusterType.KAFKA
              ? confluentLocalKafkaClusterStrategy : confluentLocalSchemaRegistryClusterStrategy;
      case PLATFORM -> null;
    };
  }
}
