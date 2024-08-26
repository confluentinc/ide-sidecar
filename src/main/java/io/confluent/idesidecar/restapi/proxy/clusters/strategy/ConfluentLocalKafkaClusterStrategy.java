package io.confluent.idesidecar.restapi.proxy.clusters.strategy;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Strategy for processing requests and responses for a local Kafka cluster.
 */
@ApplicationScoped
public class ConfluentLocalKafkaClusterStrategy extends ClusterStrategy {

  final String confluentLocalKafkaRestHostname;

  public ConfluentLocalKafkaClusterStrategy(
      @ConfigProperty(name = "ide-sidecar.connections.confluent-local.default.kafkarest-hostname")
      String confluentLocalKafkaRestHostname
  ) {
    this.confluentLocalKafkaRestHostname = confluentLocalKafkaRestHostname;
  }


  @Override
  public String constructProxyUri(String requestUri, String clusterUri) {
    // Remove the /kafka prefix from the request URI since this is how the REST API
    // running in Confluent Local Kafka is configured.
    return uriUtil.combine(clusterUri, requestUri.replaceFirst("^(/kafka|kafka)", ""));
  }

  @Override
  public String processProxyResponse(String proxyResponse, String clusterUri, String sidecarHost) {
    return super.processProxyResponse(
        proxyResponse,
        // We don't care for the clusterUri passed to this method since it
        // points to the externally accessible URI of the Confluent Local Kafka REST API
        // exposed from the running docker container.
        // Instead, the response contains the docker container's internal hostname ("rest-proxy"),
        // which is what we want to replace with the sidecar URI.
        "http://%s".formatted(confluentLocalKafkaRestHostname),
        // Since the REST API running in Confluent Local Kafka is configured to run on /v3,
        // we add the /kafka to the sidecar URI before replacing the cluster URI
        // with the sidecar URI.
        uriUtil.combine(sidecarHost, "/kafka")
    );
  }
}
