package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.application.ProxyProcessorBeanProducers;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JksOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Processor that handles the cluster proxying logic. This is the last Kafka processor before
 * handing off to the {@link io.confluent.idesidecar.restapi.proxy.ClusterProxyRequestProcessor} to
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

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  @Inject
  WebClientFactory webClientFactory;

  @Override
  public Future<ClusterProxyContext> process(ClusterProxyContext context) {
    Log.info("Start ClusterProxyProcessor");
    // Set everything needed to do the proxy request
    var requestStrategy = context.getClusterStrategy();
    context.setProxyRequestAbsoluteUrl(
        requestStrategy.constructProxyUri(context.getRequestUri(), context.getClusterInfo().uri())
    );
    Log.infof("ProxyRequestAbsoluteUrl: %s", context.getProxyRequestAbsoluteUrl());
    context.setProxyRequestHeaders(
        requestStrategy.constructProxyHeaders(context));
    Log.infof("ProxyRequestHeaders: %s", context.getProxyRequestHeaders());

    // Pass request method and body straight through, no processing needed
    context.setProxyRequestMethod(context.getRequestMethod());
    Log.infof("ProxyRequestMethod: %s", context.getProxyRequestMethod());
    context.setProxyRequestBody(context.getRequestBody());
    Log.infof("ProxyRequestBody: %s", context.getProxyRequestBody());

    Log.info("End ClusterProxyProcessor");
    return next().process(context).map(
        processedContext -> {
          if (processedContext.getProxyResponseBody() != null) {
            Log.infof("ProxyResponseBody: %s", processedContext.getProxyResponseBody().toString());
            var processedResponseBody = requestStrategy.processProxyResponse(
                processedContext.getProxyResponseBody().toString(),
                context.getClusterInfo().uri(),
                sidecarHost);
            context.setProxyResponseBody(Buffer.buffer(processedResponseBody));
          }
          return processedContext;
        });
  }
}
