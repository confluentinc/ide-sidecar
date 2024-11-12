package io.confluent.idesidecar.restapi.featureflags;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.launchdarkly.sdk.ContextKind;
import com.launchdarkly.sdk.LDContext;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.MockitoConfig;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
class FeatureFlagsContextTest extends BaseFeatureFlagsTest implements FeatureFlagTestConstants {

  @InjectMock
  @MockitoConfig(convertScopes = true)
  SidecarInfo sidecar;

  CCloudConnectionState ccloudState;

  @Inject
  WebClientFactory webClientFactory;

  @BeforeEach
  void beforeEach() {
  }

  @Test
  void shouldInitializeWithDeviceContext() {
    // When newly initialized without a sidecar
    var flags = new FeatureFlags();
    flags.webClientFactory = webClientFactory;
    flags.sidecar = null;

    // Then the device context should be minimal
    assertDeviceContextMatchesNoSidecar(flags);

    // And there should not be a CCloud context
    assertNoCCloudContext(flags);

    // When we set the sidecar info and the startup event method is called
    flags.sidecar = sidecar;
    expectSidecarInfo();
    await(
        flags.initializeAndWait()
    );

    // Then the device context should contain the sidecar information
    assertDeviceContextMatchesSidecar(flags);

    // And there should still not be a CCloud context
    assertNoCCloudContext(flags);
  }

  @Test
  void shouldUpdateWithCCloudContext() {
    // When newly initialized with a sidecar and the startup event method is called
    var flags = new FeatureFlags();
    flags.webClientFactory = webClientFactory;
    flags.sidecar = this.sidecar;
    expectSidecarInfo();
    await(
        flags.initializeAndWait()
    );

    // And a connection to CCloud is established
    expectCCloudState();
    flags.onConnectionConnected(ccloudState);

    // Then the device context should contain the sidecar information
    assertDeviceContextMatchesSidecar(flags);

    // And the CCloud context should contain the CCloud information
    assertCCloudContext(flags);

    //When the CCloud connection is updated
    flags.onConnectionUpdated(ccloudState);

    // Then the device context should contain the sidecar information
    assertDeviceContextMatchesSidecar(flags);

    // And the CCloud context should contain the CCloud information
    assertCCloudContext(flags);

    // When the CCloud connection is disconnected
    flags.onConnectionDisconnected(ccloudState);

    // Then the device context should contain the sidecar information
    assertDeviceContextMatchesSidecar(flags);

    // And there should still not be a CCloud context
    assertNoCCloudContext(flags);
  }

  void expectSidecarInfo() {
    when(sidecar.osName()).thenReturn(OS_NAME);
    when(sidecar.osVersion()).thenReturn(OS_VERSION);
    when(sidecar.osType()).thenReturn(OS_TYPE);
    when(sidecar.vsCode()).thenReturn(Optional.of(VS_CODE));
    when(sidecar.version()).thenReturn(SIDECAR_VERSION);
  }

  void expectCCloudState() {
    var userDetails = mock(CCloudOAuthContext.UserDetails.class);
    when(userDetails.resourceId()).thenReturn(USER_ID);
    when(userDetails.email()).thenReturn(USER_EMAIL);
    var orgDetails = mock(CCloudOAuthContext.OrganizationDetails.class);
    when(orgDetails.resourceId()).thenReturn(ORG_ID);
    var oauthContext = mock(CCloudOAuthContext.class);
    when(oauthContext.getUser()).thenReturn(userDetails);
    when(oauthContext.getCurrentOrganization()).thenReturn(orgDetails);

    ccloudState = mock(CCloudConnectionState.class);
    when(ccloudState.getOauthContext()).thenReturn(oauthContext);
  }

  void assertDeviceContextMatchesNoSidecar(FeatureFlags flags) {
    var deviceContext = flags.deviceContext();
    assertNotNull(deviceContext);

    assertEquals(FeatureFlags.DEVICE_CONTEXT_KIND, deviceContext.getKind());
    assertEquals(FeatureFlags.DEVICE_UUID, deviceContext.getValue("id").stringValue());
    assertAttributeNames(deviceContext, "id");
  }

  void assertDeviceContextMatchesSidecar(FeatureFlags flags) {
    var deviceContext = flags.deviceContext();
    assertNotNull(deviceContext);

    assertEquals(FeatureFlags.DEVICE_CONTEXT_KIND, deviceContext.getKind());
    assertEquals(FeatureFlags.DEVICE_UUID, deviceContext.getKey());
    assertEquals(FeatureFlags.DEVICE_UUID, deviceContext.getValue("id").stringValue());

    assertEquals(OS_NAME, deviceContext.getValue("os.name").stringValue());
    assertEquals(OS_VERSION, deviceContext.getValue("os.version").stringValue());
    assertEquals(OS_TYPE.name(), deviceContext.getValue("os.type").stringValue());
    assertEquals(VS_CODE.extensionVersion(), deviceContext.getValue("vscode.extension.version").stringValue());
    assertEquals(VS_CODE.version(), deviceContext.getValue("vscode.version").stringValue());
    assertEquals(SIDECAR_VERSION, deviceContext.getValue("sidecar.version").stringValue());
    assertAttributeNames(
        deviceContext,
        "id",
        "os.name",
        "os.version",
        "os.type",
        "sidecar.version",
        "vscode.extension.version",
        "vscode.version"
    );
  }

  void assertNoCCloudContext(FeatureFlags flags) {
    assertSame(flags.deviceContext(), flags.ccloudContext());
  }

  void assertCCloudContext(FeatureFlags flags) {
    var ccloudContext = flags.ccloudContext();
    assertNotNull(ccloudContext);

    assertEquals(ContextKind.MULTI, ccloudContext.getKind());
    var userContext = ccloudContext.getIndividualContext(FeatureFlags.USER_CONTEXT_KIND);
    var deviceContext = ccloudContext.getIndividualContext(FeatureFlags.DEVICE_CONTEXT_KIND);

    assertSame(flags.deviceContext(), deviceContext);

    assertEquals(FeatureFlags.USER_CONTEXT_KIND, userContext.getKind());
    assertEquals(USER_ID, userContext.getKey());
    assertEquals(USER_ID, userContext.getValue("user.resource_id").stringValue());
    assertEquals(ORG_ID, userContext.getValue("org.resource_id").stringValue());
    assertEquals(USER_EMAIL, userContext.getValue("email").stringValue());
    assertAttributeNames(
        userContext,
        "user.resource_id",
        "org.resource_id",
        "email"
    );

  }

  void assertAttributeNames(LDContext context, String...attributeNames) {
    var existingNames = StreamSupport.stream(
        context.getCustomAttributeNames().spliterator(),
        false
    ).collect(Collectors.toSet());

    var expectedNames = Set.of(attributeNames);
    assertEquals(expectedNames, existingNames);
  }
}
