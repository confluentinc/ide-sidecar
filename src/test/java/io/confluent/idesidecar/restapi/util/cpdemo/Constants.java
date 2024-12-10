package io.confluent.idesidecar.restapi.util.cpdemo;

public class Constants {
  private Constants() {
  }

  static String SSL_CIPHER_SUITES = "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

  public static final String BROKER_JAAS_CONTENTS = """
      Client {
             org.apache.zookeeper.server.auth.DigestLoginModule required
             username="kafka"
             password="kafkasecret";
      };

      internalhost.KafkaServer {
           org.apache.kafka.common.security.plain.PlainLoginModule required
           username="admin"
           password="admin-secret"
           user_admin="admin-secret"
           user_mds="mds-secret";
      };

      tokenhost.KafkaServer {
           org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
           publicKeyPath="/tmp/conf/public.pem";
      };
      """;

}
