package io.confluent.idesidecar.restapi.cache;

import static jakarta.ws.rs.core.HttpHeaders.AUTHORIZATION;

import io.confluent.idesidecar.restapi.application.SidecarAccessTokenBean;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Create an ApplicationScoped bean to cache SchemaRegistryClient instances
 * by connection ID and schema registry client ID.
 */
@ApplicationScoped
public class SchemaRegistryClients extends Clients<SchemaRegistryClient> {
  private static final int SR_CACHE_SIZE = 10;

  @Inject
  SidecarAccessTokenBean accessTokenBean;

  @ConfigProperty(name = "ide-sidecar.api.host")
  String sidecarHost;

  /**
   * Get a SchemaRegistryClient for the given connection ID and cluster ID. We rely on the
   * sidecar's Schema Registry proxy routes to forward the request to the correct Schema Registry
   * instance.
   */
  public SchemaRegistryClient getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> createClient(
            sidecarHost,
            Map.of(
                RequestHeadersConstants.CONNECTION_ID_HEADER, connectionId,
                RequestHeadersConstants.CLUSTER_ID_HEADER, clusterId,
                AUTHORIZATION, "Bearer %s".formatted(accessTokenBean.getToken())
            )
        ));
  }

  private SchemaRegistryClient createClient(
      String srClusterUri,
      Map<String, String> headers
  ) {
    return new CachedSchemaRegistryClient(
        Collections.singletonList(srClusterUri),
        SR_CACHE_SIZE,
        Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()
        ),
        Collections.emptyMap(),
        headers
    );
  }
}
