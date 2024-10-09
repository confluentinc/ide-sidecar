package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Registers external classes for reflection.
 *
 * @see <a
 * href="https://quarkus.io/guides/writing-native-applications-tips#registering-for-reflection">Registering
 * for reflection</a>
 */
@RegisterForReflection(
    targets = {
        ByteArrayDeserializer.class,
        CooperativeStickyAssignor.class,
        RangeAssignor.class,
        SchemaString.class,
        TemplateManifest.class
    }
)
public class ReflectionConfiguration {

}
