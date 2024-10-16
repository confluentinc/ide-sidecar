package io.confluent.idesidecar.restapi.util;

import java.util.Locale;

public enum OperatingSystemType {
  Windows,
  MacOS,
  Linux,
  Other;

  public interface Properties {
    String getProperty(String key, String defaultValue);
  }

  @SuppressWarnings("BooleanExpressionComplexity")
  public static OperatingSystemType from(Properties system) {
    var osName = system.getProperty("os.name", "unknown");

    // Determine the best-matching OS type
    var lowerName = osName.toLowerCase(Locale.US);
    if (lowerName.contains("windows")) {
      return OperatingSystemType.Windows;
    }
    if (lowerName.contains("mac") || lowerName.contains("darwin")) {
      return OperatingSystemType.MacOS;
    }
    if (lowerName.contains("nix") || lowerName.contains("nux") || lowerName.contains("ubuntu")
        || lowerName.contains("centos") || lowerName.contains("aix")) {
      return OperatingSystemType.Linux;
    }
    return OperatingSystemType.Other;
  }

  public static OperatingSystemType current() {
    return from(System::getProperty);
  }
}
