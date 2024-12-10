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
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.DirectKafkaClusterStrategy;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.DirectSchemaRegistryClusterStrategy;
import io.vertx.core.Future;

/**
 * This processor is responsible for choosing the correct strategy for the given cluster type and
 * connection type.
 */
public class ClusterStrategyProcessor<T extends ClusterProxyContext> extends
    Processor<T, Future<T>> {

  private final ConfluentCloudKafkaClusterStrategy confluentCloudKafkaClusterStrategy;
  private final ConfluentLocalKafkaClusterStrategy confluentLocalKafkaClusterStrategy;
  private final DirectKafkaClusterStrategy directKafkaClusterStrategy;
  private final ConfluentCloudSchemaRegistryClusterStrategy confluentCloudSchemaRegistryClusterStrategy;
  private final ConfluentLocalSchemaRegistryClusterStrategy confluentLocalSchemaRegistryClusterStrategy;
  private final DirectSchemaRegistryClusterStrategy directSchemaRegistryClusterStrategy;

  public ClusterStrategyProcessor(
      ConfluentCloudKafkaClusterStrategy confluentCloudKafkaClusterStrategy,
      ConfluentLocalKafkaClusterStrategy confluentLocalKafkaClusterStrategy,
      DirectKafkaClusterStrategy directKafkaClusterStrategy,
      ConfluentCloudSchemaRegistryClusterStrategy confluentCloudSchemaRegistryClusterStrategy,
      ConfluentLocalSchemaRegistryClusterStrategy confluentLocalSchemaRegistryClusterStrategy,
      DirectSchemaRegistryClusterStrategy directSchemaRegistryClusterStrategy) {
    this.confluentCloudKafkaClusterStrategy = confluentCloudKafkaClusterStrategy;
    this.confluentLocalKafkaClusterStrategy = confluentLocalKafkaClusterStrategy;
    this.directKafkaClusterStrategy = directKafkaClusterStrategy;
    this.confluentCloudSchemaRegistryClusterStrategy = confluentCloudSchemaRegistryClusterStrategy;
    this.confluentLocalSchemaRegistryClusterStrategy = confluentLocalSchemaRegistryClusterStrategy;
    this.directSchemaRegistryClusterStrategy = directSchemaRegistryClusterStrategy;
  }


  @Override
  public Future<T> process(T context) {
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
    return switch(clusterType) {
      case KAFKA -> switch (connectionType) {
        case CCLOUD -> confluentCloudKafkaClusterStrategy;
        case LOCAL -> confluentLocalKafkaClusterStrategy;
        case DIRECT -> directKafkaClusterStrategy;
        case PLATFORM -> null;
      };
      case SCHEMA_REGISTRY -> switch (connectionType) {
        case CCLOUD -> confluentCloudSchemaRegistryClusterStrategy;
        case LOCAL -> confluentLocalSchemaRegistryClusterStrategy;
        case DIRECT -> directSchemaRegistryClusterStrategy;
        case PLATFORM -> null;
      };
    };
  }
}
