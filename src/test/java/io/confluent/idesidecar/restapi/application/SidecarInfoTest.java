package io.confluent.idesidecar.restapi.application;

import static io.confluent.idesidecar.restapi.application.SidecarInfo.OS_NAME_KEY;
import static io.confluent.idesidecar.restapi.application.SidecarInfo.OS_VERSION_KEY;
import static io.confluent.idesidecar.restapi.application.SidecarInfo.VSCODE_EXTENSION_VERSION_KEY;
import static io.confluent.idesidecar.restapi.application.SidecarInfo.VSCODE_VERSION_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.util.OperatingSystemType;
import io.quarkus.logging.Log;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

public class SidecarInfoTest {

  record TestInputs(
      String displayName,
      String osName,
      String osVersion,
      OperatingSystemType os,
      SidecarInfo.VsCode vscode,
      String vscodeVersionPrefix
  ) {
    Map<String, String> asProperties() {
      // Construct the "system" properties from the input
      Map<String, String> props = new HashMap<>();
      props.put(OS_NAME_KEY, osName);
      props.put(OS_VERSION_KEY, osVersion);
      if (vscode != null) {
        var vscodeVersion = vscode.version();
        var vscodeExtensionVersion = vscode.extensionVersion();
        if (vscodeVersionPrefix != null) {
          vscodeVersion = vscodeVersionPrefix + vscodeVersion;
          vscodeExtensionVersion = vscodeVersionPrefix + vscodeExtensionVersion;
        }
        props.put(VSCODE_VERSION_KEY, vscodeVersion);
        props.put(VSCODE_EXTENSION_VERSION_KEY, vscodeExtensionVersion);
      }
      return props;
    }
  }

  public static SidecarInfo createSidecarInfo(TestInputs input) {
    Map<String, String> props = input.asProperties();
    return new SidecarInfo(props::getOrDefault, (key, def) -> null);
  }

  @TestFactory
  Stream<DynamicTest> testCombinations() {

    List<TestInputs> inputs = List.of(
        // Linux
        new TestInputs(
            "Linux OS info with VS Code",
            "Linux",
            "22.0413",
            OperatingSystemType.Linux,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            null
        ),
        new TestInputs(
            "Linux OS info with VS Code and version prefix",
            "Linux",
            "22.0413",
            OperatingSystemType.Linux,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            "v"
        ),
        new TestInputs(
            "Linux OS info without VS Code",
            "Linux",
            "22.0413",
            OperatingSystemType.Linux,
            null,
            null
        ),
        // Mac OS
        new TestInputs(
            "Mac OS info with VS Code",
            "Mac OS X",
            "13.1",
            OperatingSystemType.MacOS,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            ""
        ),
        new TestInputs(
            "Mac OS info with VS Code and version prefix",
            "Mac OS X",
            "13.1",
            OperatingSystemType.MacOS,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            "v"
        ),
        new TestInputs(
            "Mac OS info without VS Code",
            "Mac OS X",
            "13.1",
            OperatingSystemType.MacOS,
            null,
            null
        ),
        // Windows 10
        new TestInputs(
            "Windows 10 info with VS Code",
            "Windows 10",
            "10.0.1904562",
            OperatingSystemType.Windows,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            ""
        ),
        new TestInputs(
            "Windows 10 info with VS Code and version prefix",
            "Windows 10",
            "10.0.1904562",
            OperatingSystemType.Windows,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            "v"
        ),
        new TestInputs(
            "Windows 10 info without VS Code",
            "Windows 10",
            "10.0.1904562",
            OperatingSystemType.Windows,
            null,
            null
        ),
        // Windows 11
        new TestInputs(
            "Windows 11 info with VS Code",
            "Windows 11",
            "10.0.1904562",
            OperatingSystemType.Windows,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            ""
        ),
        new TestInputs(
            "Windows 11 info with VS Code and version prefix",
            "Windows 11",
            "10.0.1904562",
            OperatingSystemType.Windows,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            "v"
        ),
        new TestInputs(
            "Windows 11 info without VS Code",
            "Windows 11",
            "10.1.X.Y",
            OperatingSystemType.Windows,
            null,
            null
        ),
        // Other
        new TestInputs(
            "Solaris info with VS Code",
            "Solaris 4",
            "4.1.X.Y",
            OperatingSystemType.Other,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            ""
        ),
        new TestInputs(
            "Solaris info with VS Code and version prefix",
            "Solaris 4",
            "4.1.X.Y",
            OperatingSystemType.Other,
            new SidecarInfo.VsCode("20.1.2", "1.2.3"),
            "v"
        ),
        new TestInputs(
            "Solaris info without VS Code",
            "Solaris 4",
            "4.1.X.Y",
            OperatingSystemType.Other,
            null,
            null
        )
    );
    return inputs
        .stream()
        .map(input -> DynamicTest.dynamicTest(
            "Testing: " + input.displayName,
            () -> {
              // Construct a SidecarInfo object from the input
              SidecarInfo sidecar = createSidecarInfo(input);

              // Verify the output matches
              assertEquals(input.os, sidecar.osType());
              assertEquals(input.osName, sidecar.osName());
              assertEquals(input.osVersion, sidecar.osVersion());
              if (input.vscode != null) {
                assertTrue(sidecar.vsCode().isPresent());
                assertEquals(input.vscode.version(), sidecar.vsCode().get().version());
                assertEquals(input.vscode.extensionVersion(), sidecar.vsCode().get().extensionVersion());
              } else {
                assertFalse(sidecar.vsCode().isPresent());
              }
            })
        );
  }

  @Test
  void shouldEvaluateCurrentOsWithoutVSCode() {
    // Don't use the constants, so that we check that the constants are correct
    final String vscodeVersionKey = "vscode.version";
    final String vscodeExtensionVersionKey = "vscode.extension.version";
    final String existingVscodeVersion = System.getProperty(vscodeVersionKey);
    final String existingVscodeExtensionVersion = System.getProperty(vscodeExtensionVersionKey);
    try {
      System.clearProperty(vscodeVersionKey);
      System.clearProperty(vscodeExtensionVersionKey);
      SidecarInfo sidecar = new SidecarInfo();
      assertNotNull(sidecar.osType());
      assertEquals(System.getProperty("os.name"), sidecar.osName());
      assertEquals(System.getProperty("os.version"), sidecar.osVersion());
      assertFalse(sidecar.vsCode().isPresent());
      // And there is a non-other type
      assertNotNull(sidecar.osType());
      assertNotEquals(OperatingSystemType.Other, sidecar.osType());
    } finally {
      // Unset the system properties we just set
      if (existingVscodeVersion != null) {
        System.setProperty(vscodeVersionKey, existingVscodeVersion);
      }
      if (existingVscodeExtensionVersion != null) {
        System.setProperty(vscodeExtensionVersionKey, existingVscodeExtensionVersion);
      }
    }
  }

  @Test
  void shouldEvaluateCurrentOsWithVSCode() {
    final String existingVscodeVersion = System.getProperty(VSCODE_VERSION_KEY);
    final String existingVscodeExtensionVersion = System.getProperty(VSCODE_EXTENSION_VERSION_KEY);
    try {
      System.setProperty(VSCODE_VERSION_KEY, "v20.1.2");
      System.setProperty(VSCODE_EXTENSION_VERSION_KEY, "v1.2.3");
      SidecarInfo sidecar = new SidecarInfo();
      assertNotNull(sidecar.osType());
      assertEquals(System.getProperty("os.name"), sidecar.osName());
      assertEquals(System.getProperty("os.version"), sidecar.osVersion());
      assertTrue(sidecar.vsCode().isPresent());
      assertEquals("20.1.2", sidecar.vsCode().get().version());
      assertEquals("1.2.3", sidecar.vsCode().get().extensionVersion());
      // And there is a non-other type
      assertNotNull(sidecar.osType());
      assertNotEquals(OperatingSystemType.Other, sidecar.osType());
      Log.infof("Current OS info: type=%s, name='%s', version='%s'", sidecar.osType(), sidecar.osName(), sidecar.osVersion());
    } finally {
      // Unset the system properties we just set
      if (existingVscodeVersion == null) {
        System.clearProperty(VSCODE_VERSION_KEY);
      } else {
        System.setProperty(VSCODE_VERSION_KEY, existingVscodeVersion);
      }
      if (existingVscodeExtensionVersion == null) {
        System.clearProperty(VSCODE_EXTENSION_VERSION_KEY);
      } else {
        System.setProperty(VSCODE_EXTENSION_VERSION_KEY, existingVscodeExtensionVersion);
      }
    }
  }

  //@Test
  void printOsInformation() {
    System.out.println("os.name: " + System.getProperty(OS_NAME_KEY));
    System.out.println("os.version: " + System.getProperty(OS_VERSION_KEY));
  }
}
