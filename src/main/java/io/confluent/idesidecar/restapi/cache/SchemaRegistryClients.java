package io.confluent.idesidecar.restapi.cache;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Create an ApplicationScoped bean to cache SchemaRegistryClient instances
 * by connection ID and schema registry client ID.
 */
@ApplicationScoped
public class SchemaRegistryClients extends Clients<SchemaRegistryClient> {

}
