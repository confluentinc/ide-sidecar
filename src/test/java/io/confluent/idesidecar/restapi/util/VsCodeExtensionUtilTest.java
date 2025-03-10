package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStates;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
public class VsCodeExtensionUtilTest {
  @Inject
  VsCodeExtensionUtil vsCodeExtensionUtil;

  static final String DEFAULT_REDIRECT_URI = "vscode://confluentinc.vscode-confluent/authCallback";

  @Test
  void shouldReturnUriFromConnectionSpec() {
    var customRedirectUri = "vscode://custom-uri";
    var connection = ConnectionStates.from(
        ConnectionSpec.createCCloud(
            "id",
            "name",
            new CCloudConfig(
                null,
                customRedirectUri
            )
        ),
        null
    );

    var result = vsCodeExtensionUtil.getRedirectUri((CCloudConnectionState) connection);

    assertEquals(customRedirectUri, result);
  }

  @Test
  void shouldReturnDefaultUriWhenCcloudConfigIsNull() {
    var connection = ConnectionStates.from(
        ConnectionSpec.createCCloud(
            "id",
            "name",
            null
        ),
        null
    );

    var util = new VsCodeExtensionUtil();
    var result = util.getRedirectUri((CCloudConnectionState) connection);

    assertEquals(DEFAULT_REDIRECT_URI, result);
  }

  @Test
  void shouldReturnDefaultUriWhenIdeAuthCallbackUriIsNull() {
    var connection = ConnectionStates.from(
        ConnectionSpec.createCCloud(
            "id",
            "name",
            new CCloudConfig(null, null)
        ),
        null
    );

    var util = new VsCodeExtensionUtil();
    var result = util.getRedirectUri((CCloudConnectionState) connection);

    assertEquals(DEFAULT_REDIRECT_URI, result);
  }
}
