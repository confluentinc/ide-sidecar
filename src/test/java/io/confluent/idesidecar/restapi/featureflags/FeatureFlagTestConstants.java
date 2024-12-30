package io.confluent.idesidecar.restapi.featureflags;

import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.util.OperatingSystemType;

public interface FeatureFlagTestConstants {

  String SIDECAR_VERSION = "0.12.3";
  String OS_NAME = "Mac OS X";
  String OS_VERSION = "13.1";
  OperatingSystemType OS_TYPE = OperatingSystemType.MacOS;
  SidecarInfo.VsCode VS_CODE = new SidecarInfo.VsCode("20.1.2", "1.2.3", "vscode");
  String USER_ID = "u-1234";
  String USER_EMAIL = "zeus@acme.com";
  String ORG_ID = "b0362fb6-772a-44bb-9f5d-ce576f649e48";

}
