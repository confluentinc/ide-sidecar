/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.util.OperatingSystemType;
import io.confluent.idesidecar.restapi.util.OperatingSystemType.Properties;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.smallrye.common.constraint.NotNull;
import jakarta.inject.Singleton;
import java.util.Optional;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * General information about the sidecar, including its version, OS information, and
 * VS Code information (if available).
 *
 * <p>The OS information is obtained from the {@link System#getProperty system properties}:
 * <ul>
 *   <li>{@link #osName()} -- the value of the '{@value #OS_NAME_KEY}' system property</li>
 *   <li>{@link #osVersion()} -- the value of the '{@value #OS_VERSION_KEY}' system property</li>
 *   <li>{@link #osType()} -- enumeration derived from the {@value #OS_NAME_KEY} system property
 *   <li>{@link #osArch()} -- value derived from the {@value #OS_ARCH_KEY} system property
 * </ul>
 *
 * <p>The VS Code information is obtained first from the system properties if
 * <ul>
 *   <li>{@link VsCode#version()} -- the value of the '{@value #VSCODE_VERSION_KEY}' system property
 *     (e.g., {@code -Dvscode.version=0.17.1}, or if not defined from the
 *     {@value #VSCODE_VERSION_ENV} environment variable</li>
 *   <li>{@link VsCode#extensionVersion()} ()} -- the value of the '{@value #VSCODE_VERSION_KEY}'
 *     system property (e.g., {@code -Dvscode.extension.version=0.17.1}, or if not defined
 *     from the {@value #VSCODE_EXTENSION_VERSION_ENV} environment variable</li>
 * </ul>
 */
@Startup
@Singleton
public class SidecarInfo {

  /* UNSET and VERSION patterned after how determined in ...application.Main */
  static final String UNSET_VERSION = "unset";

  public record VsCode(
      String version,
      String extensionVersion
  ) {
  }

  static final Pattern SEMANTIC_VERSION_FROM = Pattern.compile("(\\d+[.]\\d+([.]\\d+)?)");

  static String semanticVersionWithin(Properties props, String key, String def) {
    var value = props.getProperty(key, def);
    if (value == null) {
      return null;
    }
    var matcher = SEMANTIC_VERSION_FROM.matcher(value);
    return matcher.find() ? matcher.group(1) : value;
  }

  static final String OS_ARCH_KEY = "os.arch";
  static final String OS_NAME_KEY = "os.name";
  static final String OS_VERSION_KEY = "os.version";
  static final String VERSION = ConfigProvider
      .getConfig()
      .getOptionalValue("quarkus.application.version", String.class)
      .orElse(UNSET_VERSION);;
  static final String VSCODE_VERSION_ENV = "VSCODE_VERSION";
  static final String VSCODE_VERSION_KEY = "vscode.version";
  static final String VSCODE_EXTENSION_VERSION_ENV = "VSCODE_EXTENSION_VERSION";
  static final String VSCODE_EXTENSION_VERSION_KEY = "vscode.extension.version";

  private final String osArch;
  private final OperatingSystemType osType;
  private final String osName;
  private final String sidecarVersion;
  private final String osVersion;
  private final Optional<VsCode> vscode;

  public SidecarInfo() {
    this(
        System::getProperty,
        (key, def) -> {
          var result = System.getenv(key);
          return result != null ? result : def;
        }
    );
  }

  SidecarInfo(@NotNull Properties system, @NotNull Properties env) {

    // Get the OS information
    osName = system.getProperty(OS_NAME_KEY, "unknown");
    osVersion = system.getProperty(OS_VERSION_KEY, "unknown");
    sidecarVersion =  system.getProperty(VERSION, UNSET_VERSION);
    osArch =  system.getProperty(OS_ARCH_KEY, "unknown");

    // Determine the best-matching OS type
    osType = OperatingSystemType.from(system);

    // Set the VS Code information if available
    var vscodeVersion = semanticVersionWithin(system, VSCODE_VERSION_KEY, null);
    if (vscodeVersion == null) {
      vscodeVersion = semanticVersionWithin(env, VSCODE_VERSION_ENV, null);
    }
    var vscodeExtensionVersion = semanticVersionWithin(system, VSCODE_EXTENSION_VERSION_KEY, null);
    if (vscodeExtensionVersion == null) {
      vscodeExtensionVersion = semanticVersionWithin(env, VSCODE_EXTENSION_VERSION_ENV, null);
    }
    if (vscodeVersion != null) {
      vscode = Optional.of(new VsCode(vscodeVersion, vscodeExtensionVersion));
    } else {
      vscode = Optional.empty();
    }

    Log.info(this);
  }

  public String version() {
    return sidecarVersion;
  }

  public OperatingSystemType osType() {
    return osType;
  }

  public String osName() {
    return osName;
  }

  public String osVersion() {
    return osVersion;
  }

  public Optional<VsCode> vsCode() {
    return vscode;
  }

  public String osArch() {
    return osArch;
  }

  @Override
  public String toString() {
    return "OS: %s %s (%s); VS Code %s, extension version %s".formatted(
        osName,
        osVersion,
        osType.name(),
        vsCode().map(VsCode::version).orElse("unknown"),
        vsCode().map(VsCode::extensionVersion).orElse("unknown")
    );
  }

  static String getSystemOrEnvProperty(String name) {
    var result = System.getProperty(name);
    return result != null ? result : System.getenv(name);
  }

  public String getUserAgent() {
   return "Confluent-for-VSCode/v%s (https://confluent.io; support@confluent.io) sidecar/v%s (%s/%s)".formatted(
        vsCode().map(VsCode::extensionVersion).orElse("unknown"),
        version(),
        osType().name().toLowerCase(),
        osArch()
   );
  }
}
