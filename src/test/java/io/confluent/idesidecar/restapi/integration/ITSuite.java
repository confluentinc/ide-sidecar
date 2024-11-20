package io.confluent.idesidecar.restapi.integration;

import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.util.SidecarClient;
import io.confluent.idesidecar.restapi.util.SidecarClientApi;
import io.confluent.idesidecar.restapi.util.TestEnvironment;

public interface ITSuite extends SidecarClientApi {

  TestEnvironment environment();

  SidecarClient sidecarClient();

  SimpleConsumer simpleConsumer();

  void setupConnection();
}
