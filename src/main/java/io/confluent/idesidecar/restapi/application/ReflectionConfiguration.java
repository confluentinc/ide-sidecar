package io.confluent.idesidecar.restapi.application;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.sun.security.auth.module.JndiLoginModule;
import com.sun.security.auth.module.KeyStoreLoginModule;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.ExtendedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryServerVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTags;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaTypeConverter;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity.EntityType;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
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
        ScramServerCallbackHandler.class,
        // Schema Registry client classes that are not registered in
        // https://github.com/quarkusio/quarkus/blob/3.16.3/extensions/schema-registry/confluent/common/deployment/src/main/java/io/quarkus/confluent/registry/common/ConfluentRegistryClientProcessor.java
        DefaultReferenceSubjectNameStrategy.class,
        SchemaEntity.class,
        SchemaReference.class,
        SchemaRegistryServerVersion.class,
        SchemaTags.class,
        ServerClusterId.class,
        SubjectVersion.class,
        Mode.class,
        Rule.class,
        RuleKind.class,
        RuleMode.class,
        Metadata.class,
        ExtendedSchema.class,
        TagSchemaRequest.class,
        ConfigUpdateRequest.class,
        CompatibilityCheckResponse.class,
        ModeUpdateRequest.class,
        RegisterSchemaResponse.class,
        RuleSet.class,
        Config.class,
        EntityType.class,
        Any.class,
    }
)
public class ReflectionConfiguration {

}
