package io.confluent.idesidecar.scaffolding.util;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PortablePathUtil {

  private PortablePathUtil() {
    // Utility class
  }

  private static String portablePath(String path) {
    // Replace both forward and backward slashes with the platform-specific separator
    return path
        .replace("\\\\", File.separator)
        .replace("//", File.separator)
        .replace("/", File.separator)
        .replace("\\", File.separator);
  }

  /**
   * Converts the provided paths to a portable path representation. Why use {@code File.separator}?
   * I'll let user "Pointy" from StackOverflow explain:
   * <a href="https://stackoverflow.com/a/2417515">https://stackoverflow.com/a/2417515</a>
   *
   * @param paths the paths to convert
   * @return the portable path
   */
  public static String portablePath(String... paths) {
    return portablePath(Stream
        .of(paths)
        .map(PortablePathUtil::portablePath)
        .collect(Collectors.joining(File.separator)));
  }
}
