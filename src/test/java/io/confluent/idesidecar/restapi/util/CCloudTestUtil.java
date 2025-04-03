package io.confluent.idesidecar.restapi.util;

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpecBuilder;
import io.vertx.core.json.JsonObject;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Optional;

public class CCloudTestUtil {

  final WireMock wireMock;
  final ConnectionStateManager connectionStateManager;
  static final UuidFactory uuidFactory = new UuidFactory();
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final UriUtil uriUtil = new UriUtil();

  public CCloudTestUtil(WireMock wireMock, ConnectionStateManager connectionStateManager) {
    this.wireMock = wireMock;
    this.connectionStateManager = connectionStateManager;
  }

  private static String getRandomString() {
    return uuidFactory.getRandomUuid();
  }

  public void registerWireMockRoutesForCCloudOAuth(String authorizationCode) {
    registerWireMockRoutesForCCloudOAuth(authorizationCode, null, null);
  }

  public AccessToken registerWireMockRoutesForCCloudOAuth(
      String authorizationCode, String ccloudOrganizationName, String ccloudOrganizationId) {
    var accessToken = AccessToken.newToken();

    expectTokenExchangeRequest(authorizationCode, accessToken);
    var controlPlaneToken = expectControlPlaneAndDataPlaneTokenExchangeRequest(
        accessToken.id_token,
        ccloudOrganizationName,
        ccloudOrganizationId
    );
    expectCheckJwtRequest(controlPlaneToken.token);

    return accessToken;
  }

  public void expectCheckJwtRequest(String controlPlaneToken) {
    wireMock.register(
        WireMock
            .get("/api/check_jwt")
            .withHeader("Authorization", equalTo("Bearer %s".formatted(controlPlaneToken)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withBody(loadResource("ccloud-oauth-mock-responses/check-jwt.json")))
            .atPriority(100));
  }

  private void expectDataPlaneTokenExchangeRequest(String controlPlaneToken) {
    var dataPlaneToken = createDataPlaneToken();
    wireMock.register(
        WireMock
            .post("/api/access_tokens")
            .withHeader("Authorization", equalTo("Bearer %s".formatted(controlPlaneToken)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody(MAPPER.valueToTree(dataPlaneToken).toString())).atPriority(100));
  }

  public void expectTokenExchangeRequest(String authorizationCode, AccessToken accessToken) {
    wireMock.register(
        WireMock
            .post("/oauth/token")
            .withRequestBody(containing("code=%s".formatted(authorizationCode)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody(MAPPER.valueToTree(accessToken).toString()))
            .atPriority(100));
  }

  public AccessToken expectRefreshTokenExchangeRequest(String refreshToken) {
    var refreshedAccessToken = AccessToken.newToken();
    wireMock.register(
        WireMock
            .post("/oauth/token")
            .withRequestBody(
                containing("refresh_token=%s".formatted(refreshToken)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody(MAPPER.valueToTree(refreshedAccessToken).toString()))
            .atPriority(100));
    return refreshedAccessToken;
  }

  public ControlPlaneTokenAndUserAndOrganization expectControlPlaneAndDataPlaneTokenExchangeRequest(
      String idToken, String ccloudOrganizationName, String ccloudOrganizationId
  ) {
    var controlPlaneToken = expectControlPlaneTokenExchangeRequest(
        idToken,
        ccloudOrganizationName,
        ccloudOrganizationId
    );
    expectDataPlaneTokenExchangeRequest(controlPlaneToken.token);
    return controlPlaneToken;
  }


  public void expectInvalidResourceIdOnControlPlaneTokenExchange(
      String idToken, String ccloudOrganizationId
  ) {
    JsonObject expectedIdTokenExchangeRequest = new JsonObject()
        .put("id_token", idToken);
    Optional.ofNullable(ccloudOrganizationId).ifPresent(
        orgId -> expectedIdTokenExchangeRequest.put("org_resource_id", orgId)
    );

    wireMock.register(
        WireMock
            .post("/api/sessions")
            .withRequestBody(equalToJson(expectedIdTokenExchangeRequest.toString()))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(400)
                    .withBody("{\"error\": {\"code\":400,\"message\":\"invalid resource id\"}}"))
    );
  }

  public ControlPlaneTokenAndUserAndOrganization expectControlPlaneTokenExchangeRequest(
      String idToken, String ccloudOrganizationName, String ccloudOrganizationId
  ) {
    JsonObject expectedIdTokenExchangeRequest = new JsonObject()
        .put("id_token", idToken);
    Optional.ofNullable(ccloudOrganizationId).ifPresent(
        orgId -> expectedIdTokenExchangeRequest.put("org_resource_id", orgId)
    );

    // We need to inject the control plane token and surface to caller. This will help us
    // to register connection-specific API call mocks.
    var controlPlaneToken = createControlPlaneToken(ccloudOrganizationName, ccloudOrganizationId);
    wireMock.register(
        WireMock
            .post("/api/sessions")
            .withRequestBody(equalToJson(expectedIdTokenExchangeRequest.toString()))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(201)
                    .withBody(MAPPER.valueToTree(controlPlaneToken).toString())));
    return controlPlaneToken;
  }

  private static ControlPlaneTokenAndUserAndOrganization createControlPlaneToken(
      String ccloudOrganizationName, String ccloudOrganizationId
  ) {
    var controlPlaneTokenResourceString = loadResource(
        "ccloud-oauth-mock-responses/control-plane-token.json");
    ControlPlaneTokenAndUserAndOrganization controlPlaneTokenResource;
    try {
      controlPlaneTokenResource = MAPPER.readValue(controlPlaneTokenResourceString,
          ControlPlaneTokenAndUserAndOrganization.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return controlPlaneTokenResource
        .withToken(getRandomString())
        .withOrganization(ccloudOrganizationName, ccloudOrganizationId);
  }

  private static DataPlaneToken createDataPlaneToken() {
    return new DataPlaneToken(null, getRandomString());
  }

  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  public AccessToken authenticateCCloudConnection(
      String connectionId, String ccloudOrganizationName, String ccloudOrganizationId) {
    // Assume user completes sign-in with their identity provider and the callback is called
    String authorizationCode = getRandomString();
    var accessToken = registerWireMockRoutesForCCloudOAuth(
        authorizationCode, ccloudOrganizationName, ccloudOrganizationId
    );
    var connectionState = connectionStateManager.getConnectionState(connectionId);
    given()
        .queryParam("code", authorizationCode)
        .queryParam("state", connectionState.getInternalId())
        .when()
        .get("/gateway/v1/callback-vscode-docs")
        .then()
        .statusCode(200)
        .body(containsString("Authentication Complete"));

    return accessToken;
  }

  public ConnectionSpec createCCloudConnection(
      String connectionId,
      String connectionName,
      CCloudConfig ccloudConfig
  ) {
    given()
        .when()
        .body(
            ConnectionSpec.createCCloud(
                connectionId,
                connectionName,
                ccloudConfig
            )
        )
        .contentType(MediaType.APPLICATION_JSON)
        .post("/gateway/v1/connections")
        .then()
        .statusCode(200);
    return connectionStateManager.getConnectionSpec(connectionId);
  }

  public ConnectionSpec createConnection(
      ConnectionSpec spec
  ) {
    given()
        .when()
        .body(spec)
        .contentType(MediaType.APPLICATION_JSON)
        .post("/gateway/v1/connections")
        .then()
        .statusCode(200);
    return connectionStateManager.getConnectionSpec(spec.id());
  }

  public ConnectionSpec createConnection(
      String connectionId,
      String connectionName,
      ConnectionType connectionType
  ) {
    return createConnection(
        ConnectionSpecBuilder
            .builder()
            .id(connectionId)
            .name(connectionName)
            .type(connectionType)
            .build()
    );
  }

  public void createAuthedConnection(String connectionId, ConnectionType connectionType) {
    createAuthedConnection(connectionId, "My Connection", connectionType);
  }

  public void createAuthedConnection(
      String connectionId,
      String connectionName,
      ConnectionType connectionType
  ) {
    if (connectionType == ConnectionType.CCLOUD) {
      createAuthedCCloudConnection(connectionId, connectionName);
    } else if (connectionType == ConnectionType.DIRECT) {
      fail(
          "Unable to create a direct connection without specifying the cluster and schema registry");
    } else {
      createConnection(connectionId, connectionName, connectionType);
    }
  }

  public AccessToken createAuthedCCloudConnection(
      String connectionId,
      String connectionName,
      String ccloudOrganizationName,
      String ccloudOrganizationId
  ) {
    createCCloudConnection(
        connectionId,
        connectionName,
        new CCloudConfig(ccloudOrganizationId, null)
    );
    return authenticateCCloudConnection(connectionId, ccloudOrganizationName, ccloudOrganizationId);
  }


  public AccessToken createAuthedCCloudConnection(
      String connectionId,
      String connectionName
  ) {
    createConnection(
        connectionId,
        connectionName,
        ConnectionType.CCLOUD
    );
    return authenticateCCloudConnection(connectionId, "Development Org", null);
  }

  public void expectSuccessfulCCloudGet(String url, String bearerToken, String resourceFilename) {
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(url))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withBody(
                        loadResource(resourceFilename)
                    )
            )
    );
  }

  public String getControlPlaneToken(String connectionId) {
    try {
      return ((CCloudConnectionState) connectionStateManager.getConnectionState(connectionId))
          .getOauthContext().getControlPlaneToken().token();
    } catch (ConnectionNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  // Auth models
  public record AccessToken(
      String access_token,
      String refresh_token,
      String id_token,
      String scope,
      long expires_in,
      String token_type
  ) {

    public static AccessToken newToken() {
      return new AccessToken(
          getRandomString(),
          getRandomString(),
          getRandomString(),
          "openid email offline_access",
          86400,
          "Bearer"
      );
    }
  }

  // Copied from CCloudOAuthContext
  public record ControlPlaneTokenAndUserAndOrganization(
      String token,
      JsonNode error,
      JsonNode user,
      Organization organization,
      @JsonProperty(value = "refresh_token") String refreshToken,
      @JsonProperty(value = "identity_provider") JsonNode identityProvider) {

    public ControlPlaneTokenAndUserAndOrganization withToken(String token) {
      return new ControlPlaneTokenAndUserAndOrganization(token, error, user, organization,
          refreshToken, identityProvider);
    }

    public ControlPlaneTokenAndUserAndOrganization withOrganization(
        String name,
        String resourceId
    ) {
      var randomId = (long) ((Math.random() * 900000) + 100000);
      return new ControlPlaneTokenAndUserAndOrganization(
          token,
          error,
          user,
          new Organization(
              randomId,
              Optional.ofNullable(name).orElse(organization().name()),
              Optional.ofNullable(resourceId).orElse(organization().resourceId())
          ),
          refreshToken,
          identityProvider
      );
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Organization(
        long id,
        String name,
        @JsonProperty("resource_id") String resourceId
    ) {

    }
  }

  public record DataPlaneToken(
      JsonNode error,
      String token
  ) {

  }
}

