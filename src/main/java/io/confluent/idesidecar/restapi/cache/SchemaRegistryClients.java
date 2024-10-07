package io.confluent.idesidecar.restapi.cache;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SchemaRegistryClients extends Clients<SchemaRegistryClient> {

}
