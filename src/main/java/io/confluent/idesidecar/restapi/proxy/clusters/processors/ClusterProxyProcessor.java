package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.application.ProxyProcessorBeanProducers;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ProxyRequestProcessor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Processor that handles the cluster proxying logic. This is the last Kafka processor before
 * handing off to the {@link ProxyRequestProcessor} to
 * make the actual request to the Kafka cluster (Kafka REST), and the first processor to handle the
 * response from the Kafka cluster.
 * <p>
 * This processor is responsible for the following:
 * <ul>
 *   <li>The proxy request is set up by setting the absolute URL, headers, method, and body.</li>
 *   <li>The response is processed by replacing the Kafka cluster URL with the sidecar URL.</li>
 * </ul>
 * </p>
 *
 * <p>See
 * {@link
 * ProxyProcessorBeanProducers#clusterProxyProcessor} for
 * the processor chain that includes this processor.</p>
 */
@ApplicationScoped
public class ClusterProxyProcessor extends
    Processor<ClusterProxyContext, Future<ClusterProxyContext>> {

  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    // Set everything needed to do the proxy request
    var requestStrategy = context.getClusterStrategy();
    context.setProxyRequestAbsoluteUrl(
        requestStrategy.constructProxyUri(context.getRequestUri(), context.getClusterInfo().uri())
    );
    context.setProxyRequestHeaders(
        requestStrategy.constructProxyHeaders(context));

    // Pass request method and body straight through, no processing needed
    context.setProxyRequestMethod(context.getRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());

    return next().process(context).map(
        processedContext -> {
          if (processedContext.getProxyResponseBody() != null) {
            var processedResponseBody = requestStrategy.processProxyResponse(
                context.getProxyResponseBody().toString()
            );
            context.setProxyResponseBody(Buffer.buffer(processedResponseBody));
          }
          return processedContext;
        });
  }
}
