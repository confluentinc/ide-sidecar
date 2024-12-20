package io.confluent.idesidecar.restapi.util.cpdemo;

import org.eclipse.microprofile.config.ConfigProvider;

public class Constants {
  private Constants() {
  }

  static String SSL_CIPHER_SUITES = "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256";

  static final String DEFAULT_CONFLUENT_DOCKER_TAG = ConfigProvider
      .getConfig()
      .getValue("ide-sidecar.integration-tests.cp-demo.tag", String.class)
      // Remove the leading 'v' from the tag, if present
      .replaceFirst("v", "");
}
