package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Processor that fetches the cluster info from the cache and updates the context with it. This is a
 * pre-processor for the {@link ClusterProxyProcessor}, which uses the cluster info to build the
 * absolute URL for the request.
 */
@ApplicationScoped
public class ClusterInfoProcessor extends
    Processor<ClusterProxyContext, Future<ClusterProxyContext>> {

  @Inject
  ClusterCache clusterCache;

  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    var clusterId = context.getClusterId();
    if (clusterId == null) {
      if (context.getClusterType().equals(ClusterType.SCHEMA_REGISTRY)) {
        return Future.failedFuture(
            new ProcessorFailedException(
                context.failf(
                    400,
                    "%s header is required",
                    RequestHeadersConstants.CLUSTER_ID_HEADER)
            )
        );
      } else {
        // This code path will never be hit for the Kafka REST Proxy since the endpoint handler
        // only matches when the cluster id is in the path. We keep it here for completeness.
        // (Code coverage crying in the corner...)
        return Future.failedFuture(
            new ProcessorFailedException(
                context.fail(400, "Cluster id path parameter is required")));
      }
    }

    // Sanity check that if we were in fact passed a `x-cluster-id` header
    // that it matches whatever is set in the context
    // (This is useful for the Kafka REST Proxy, where the cluster id is in the path,
    // not so much for the Schema Registry REST Proxy, where we expect
    // the cluster id to be in the header.)
    var clusterIdHeader = context
        .getRequestHeaders()
        .get(RequestHeadersConstants.CLUSTER_ID_HEADER);
    if (clusterIdHeader != null && !clusterIdHeader.equals(clusterId)) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.failf(
                  400,
                  "Cluster id in the header does not match the one in the path.")
          )
      );
    }

    // Load the cluster information
    var connectionId = context.getConnectionId();
    try {
      var clusterInfo = clusterCache.getCluster(connectionId, clusterId, context.getClusterType());
      context.setClusterInfo(clusterInfo);
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
