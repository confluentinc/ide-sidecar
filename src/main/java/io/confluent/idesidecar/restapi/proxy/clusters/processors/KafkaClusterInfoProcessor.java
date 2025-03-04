package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import java.util.Objects;

/**
 * Retrieves information about the Kafka and Schema Registry clusters when processing a request.
 */
public class KafkaClusterInfoProcessor<T, U> extends
    Processor<KafkaRestProxyContext<T, U>, Future<KafkaRestProxyContext<T, U>>> {
  private final ClusterCache clusterCache;

  public KafkaClusterInfoProcessor(ClusterCache cache) {
    this.clusterCache = cache;
  }

  @Override
  public Future<KafkaRestProxyContext<T, U>> process(KafkaRestProxyContext<T, U> context) {
    var clusterIdHeader = Objects
        .requireNonNullElse(context.getRequestHeaders(), MultiMap.caseInsensitiveMultiMap())
        .get(RequestHeadersConstants.CLUSTER_ID_HEADER);
    if (clusterIdHeader != null && !context.getClusterId().equals(clusterIdHeader)) {
      return Future.failedFuture(new ProcessorFailedException(
          context.fail(400, "Cluster ID in path and header do not match")
      ));
    }

    var clusterId = context.getClusterId();
    var connectionId = context.getConnectionId();
    try {
      // Get the Kafka cluster information for the given cluster ID and store on the context
      var kafkaClusterInfo = clusterCache.getKafkaCluster(
          connectionId,
          clusterId
      );
      context.setKafkaClusterInfo(kafkaClusterInfo);

      // Get info about the schema registry for that Kafka cluster and store on the context
      try {
        var schemaRegistryInfo = clusterCache.getSchemaRegistryForKafkaCluster(
            connectionId,
            kafkaClusterInfo
        );
        context.setSchemaRegistryInfo(schemaRegistryInfo);
      } catch (ClusterNotFoundException e) {
        Log.debugf("Could not find schema registry for connection with ID=%s", connectionId);
        context.setSchemaRegistryInfo(null);
      }

      // Delegate to the next processor
      return next().process(context);

    } catch (ConnectionNotFoundException | ClusterNotFoundException e) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(
                  404,
                  e.getMessage()
              )
          )
      );
    }
  }
}
