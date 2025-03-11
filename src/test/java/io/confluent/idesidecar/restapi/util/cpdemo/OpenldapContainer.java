package io.confluent.idesidecar.restapi.util.cpdemo;

import java.util.HashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class OpenldapContainer extends GenericContainer<OpenldapContainer> {

  private static final String DEFAULT_IMAGE = "osixia/openldap";
  private static final String DEFAULT_TAG = "1.3.0";
  private static final String CONTAINER_NAME = "openldap";

  public OpenldapContainer(String tag, Network network) {
    super(DEFAULT_IMAGE + ":" + tag);
    super.withNetwork(network);
    super.withNetworkAliases(CONTAINER_NAME);
    super
        .withEnv(getOpenldapEnv())
        .withCommand("--copy-service --loglevel debug");
    super.withFileSystemBind(
        ".cp-demo/scripts/security/ldap_users",
        "/container/service/slapd/assets/config/bootstrap/ldif/custom"
    );
    super.withCreateContainerCmdModifier(cmd -> cmd
        .withName(CONTAINER_NAME)
        .withHostName(CONTAINER_NAME)
    );
    super.withReuse(true);
  }

  public OpenldapContainer(Network network) {
    this(DEFAULT_TAG, network);
  }

  public Map<String, String> getOpenldapEnv() {
    var envs = new HashMap<String, String>();
    envs.put("LDAP_ORGANISATION", "ConfluentDemo");
    envs.put("LDAP_DOMAIN", "confluentdemo.io");
    envs.put("LDAP_BASE_DN", "dc=confluentdemo,dc=io");
    return envs;
  }
}
