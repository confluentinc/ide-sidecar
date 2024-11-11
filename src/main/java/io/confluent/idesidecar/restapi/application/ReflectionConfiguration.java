package io.confluent.idesidecar.restapi.application;

import com.google.protobuf.Message;
import com.sun.security.auth.module.JndiLoginModule;
import com.sun.security.auth.module.KeyStoreLoginModule;
import io.confluent.idesidecar.scaffolding.models.TemplateManifest;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.context.NullContextNameStrategy;
import io.confluent.kafka.serializers.subject.DefaultReferenceSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler;
import org.apache.kafka.common.security.kerberos.KerberosClientCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslClientCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internals.ScramServerCallbackHandler;
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
        TemplateManifest.class,
        Schema.class,
        RegisterSchemaRequest.class,
        ProtobufSchema.class,
        SchemaTypeConverter.class,
        Message.class,
        JsonSchema.class,
        AvroSchema.class,
        NullContextNameStrategy.class,
        TopicNameStrategy.class,
        ErrorMessage.class,
        DefaultReferenceSubjectNameStrategy.class,
        // Workaround for:
        // https://github.com/confluentinc/schema-registry/issues/3257
        // https://github.com/quarkusio/quarkus/issues/42845
        org.apache.avro.reflect.ReflectData.class,
        // Kafka client login module
        PlainLoginModule.class,
        ScramLoginModule.class,
        OAuthBearerLoginModule.class,
        JndiLoginModule.class,
        KeyStoreLoginModule.class,
        // Kafka client callback handler implementations
        AuthenticateCallbackHandler.class,
        PlainServerCallbackHandler.class,
        OAuthBearerLoginCallbackHandler.class,
        OAuthBearerExtensionsValidatorCallback.class,
        OAuthBearerValidatorCallback.class,
        OAuthBearerTokenCallback.class,
        KerberosClientCallbackHandler.class,
        OAuthBearerSaslClientCallbackHandler.class,
        OAuthBearerUnsecuredValidatorCallbackHandler.class,
        OAuthBearerUnsecuredLoginCallbackHandler.class,
        SaslClientCallbackHandler.class,
        ScramServerCallbackHandler.class
    }
)
public class ReflectionConfiguration {

}
