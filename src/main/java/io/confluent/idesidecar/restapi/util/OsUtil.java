package io.confluent.idesidecar.restapi.util;

/**
 * Utility for implementing OS-specific functionality.
 */
public class OsUtil {

  public enum OS {
    WINDOWS, MAC, LINUX, UNKNOWN
  }

  static final String OS_NAME = System.getProperty("os.name").toLowerCase();

  public static OS getOperatingSystem() {
    if (OS_NAME.contains("mac")) {
      return OS.MAC;
    } else if (OS_NAME.contains("win")) {
      return OS.WINDOWS;
    } else if (OS_NAME.contains("nix") || OS_NAME.contains("nux") || OS_NAME.contains("aix")) {
      return OS.LINUX;
    } else {
      return OS.UNKNOWN;
    }
  }
}
