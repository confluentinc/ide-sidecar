package io.confluent.idesidecar.restapi.resources;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.credentials.Password;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.Optional;

class ClusterProxyProcessorTest {

  @Test
  void truststorePasswordPresent() {

    var truststore = mock(TLSConfig.TrustStore.class);
    var password = mock(Password.class);

    when(password.asString(false)).thenReturn("trustpass");
    when(truststore.path()).thenReturn("/truststore.jks");
    when(truststore.password()).thenReturn(password);
    when(truststore.type()).thenReturn(TLSConfig.StoreType.JKS);

    var tlsConfig = new TLSConfig(
        true, // enabled
        true, // verifyHostname
        truststore, // your truststore mock
        null // keystore
    );
    when(tlsConfig.verifyHostname()).thenReturn(true);
    when(tlsConfig.enabled()).thenReturn(true);
    when(tlsConfig.truststore()).thenReturn(truststore);
    when(tlsConfig.keystore()).thenReturn(null);
    doCallRealMethod().when(tlsConfig).getProperties(anyBoolean());

    System.out.println("truststore.password = " + truststore.password());
    System.out.println("password.asString(false) = " + password.asString(false));
    Optional<Map<String, String>> propsOpt = tlsConfig.getProperties(false);
    assertTrue(propsOpt.isPresent());
    Map<String, String> props = propsOpt.get();
    assertEquals("trustpass", props.get("ssl.truststore.password"));
  }

  @Test
  void truststorePasswordAbsent() {
    var truststore = mock(TLSConfig.TrustStore.class);
    when(truststore.path()).thenReturn("/truststore.jks");
    when(truststore.password()).thenReturn(null);
    when(truststore.type()).thenReturn(TLSConfig.StoreType.JKS);

    var tlsConfig = mock(TLSConfig.class);
    when(tlsConfig.verifyHostname()).thenReturn(true);
    when(tlsConfig.enabled()).thenReturn(true);
    when(tlsConfig.truststore()).thenReturn(truststore);
    when(tlsConfig.keystore()).thenReturn(null);
    doCallRealMethod().when(tlsConfig).getProperties(anyBoolean());

    Optional<Map<String, String>> propsOpt = tlsConfig.getProperties(false);
    assertTrue(propsOpt.isPresent());
    Map<String, String> props = propsOpt.get();
    assertFalse(props.containsKey("ssl.truststore.password"));
  }

  @Test
  void keystorePasswordPresentAndKeyPasswordPresent() {
    var keystore = mock(TLSConfig.KeyStore.class);
    var password = mock(Password.class);
    var keyPassword = mock(Password.class);

    when(password.asString(false)).thenReturn("keypass");
    when(keyPassword.asString(false)).thenReturn("keypw");
    when(keystore.path()).thenReturn("/keystore.jks");
    when(keystore.password()).thenReturn(password);
    when(keystore.keyPassword()).thenReturn(keyPassword);
    when(keystore.type()).thenReturn(TLSConfig.StoreType.JKS);

    var tlsConfig = new TLSConfig(
        true, // enabled
        true, // verifyHostname
        null, // your truststore mock
        keystore // keystore
    );
    when(tlsConfig.verifyHostname()).thenReturn(true);
    when(tlsConfig.enabled()).thenReturn(true);
    when(tlsConfig.truststore()).thenReturn(null);
    when(tlsConfig.keystore()).thenReturn(keystore);
    doCallRealMethod().when(tlsConfig).getProperties(anyBoolean());

    Optional<Map<String, String>> propsOpt = tlsConfig.getProperties(false);
    assertTrue(propsOpt.isPresent());
    Map<String, String> props = propsOpt.get();
    assertEquals("keypass", props.get("ssl.keystore.password"));
    assertEquals("keypw", props.get("ssl.key.password"));
  }
  @Test
  void keystorePasswordAbsentAndKeyPasswordAbsent() {
    var keystore = mock(TLSConfig.KeyStore.class);
    when(keystore.path()).thenReturn("/keystore.jks");
    when(keystore.password()).thenReturn(null);
    when(keystore.keyPassword()).thenReturn(null);
    when(keystore.type()).thenReturn(TLSConfig.StoreType.JKS);

    var tlsConfig = new TLSConfig(
        true, // enabled
        true, // verifyHostname
        null, // your truststore mock
        null // keystore
    );
    when(tlsConfig.verifyHostname()).thenReturn(true);
    when(tlsConfig.enabled()).thenReturn(true);
    when(tlsConfig.truststore()).thenReturn(null);
    when(tlsConfig.keystore()).thenReturn(keystore);
    doCallRealMethod().when(tlsConfig).getProperties(anyBoolean());

    Optional<Map<String, String>> propsOpt = tlsConfig.getProperties(false);
    assertTrue(propsOpt.isPresent());
    Map<String, String> props = propsOpt.get();
    assertFalse(props.containsKey("ssl.keystore.password"));
    assertFalse(props.containsKey("ssl.key.password"));
  }
}