package io.confluent.idesidecar.restapi.application;

import org.graalvm.nativeimage.ImageSingletons;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.impl.RuntimeClassInitializationSupport;

public class ReInitFeature implements Feature {

  @Override
  public void afterRegistration(AfterRegistrationAccess access) {
    RuntimeClassInitializationSupport rci = ImageSingletons.lookup(RuntimeClassInitializationSupport.class);
    rci.initializeAtBuildTime("org.apache.kafka.common.security.kerberos.KerberosLogin", "Needs to be optimized");
    rci.rerunInitialization("org.apache.kafka.common.security.kerberos.KerberosLogin", "Contains Random instance");
  }

}