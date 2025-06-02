package io.confluent.idesidecar.restapi.proxy.clusters.processors;

import io.confluent.idesidecar.restapi.application.ProxyProcessorBeanProducers;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.clusters.ClusterProxyContext;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
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

    // Set TLS options
    var connectionState = context.getConnectionState();

    switch (context.getClusterType()) {
      case KAFKA -> {
        // Confluent Local Kafka REST Proxy is not configured with TLS.
        // However, Confluent Cloud Kafka REST does support mutual TLS. It only requires
        // the keystore options to be set. This is a TODO item for the future.
        // (https://github.com/confluentinc/ide-sidecar/issues/235)
      }
      case SCHEMA_REGISTRY -> connectionState
          .getSchemaRegistryTLSConfig()
          .ifPresent(
              tlsConfig -> {
                var options = webClientFactory.getDefaultWebClientOptions();
                if (tlsConfig.truststore() != null) {
                  var trustStore = tlsConfig.truststore();
                  var trustStoreOptions = new JksOptions()
                      .setPath(trustStore.path());
                  // Passwords are optional. We don't support passwords for PEM files.
                  if (trustStore.password() != null) {
                      trustStoreOptions.setPassword(trustStore.password().asString(false));
                  }

                  options.setTrustStoreOptions(trustStoreOptions);
                }

                if (tlsConfig.keystore() != null) {
                  var keyStore = tlsConfig.keystore();
                  var keystoreOptions = new JksOptions()
                      .setPath(keyStore.path());
                  // Passwords are optional. We don't support passwords for PEM files.
                  if (keyStore.password() != null) {
                      keystoreOptions.setPassword(keyStore.password().asString(false));
                  }

                  if (keyStore.keyPassword() != null) {
                    keystoreOptions.setAliasPassword(keyStore.keyPassword().asString(false));
                  }

                  options.setKeyStoreOptions(keystoreOptions);
                }

                context.setWebClientOptions(options);
              });
    }

    return next().process(context).map(
        processedContext -> {
          if (processedContext.getProxyResponseBody() != null) {
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
