package io.confluent.idesidecar.scaffolding.util;

import java.io.File;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PortablePathUtil {

  private PortablePathUtil() {
    // Utility class
  }

  private static String portablePath(String path) {
    var regexQuotedFileSeparator = Pattern.quote(File.separator);
    return path
        // Replace both forward and backward slashes with the platform-specific separator
        .replaceAll("[/\\\\]+", File.separator)
        // Strip leading and trailing separators
        .replaceAll(
            "^[" + regexQuotedFileSeparator + "]+|[" + regexQuotedFileSeparator + "]+$",
            "");
  }

  /**
   * Converts the provided paths to a portable path representation. Note that any leading or
   * trailing file separators will be stripped. <br>
   * Why use {@code File.separator}? I'll let user "Pointy" from StackOverflow explain:
   * <a href="https://stackoverflow.com/a/2417515">https://stackoverflow.com/a/2417515</a>
   * @param paths the paths to convert
   * @return the portable path
   */
  public static String portablePath(String... paths) {
    return Stream
        .of(paths)
        .map(PortablePathUtil::portablePath)
        .collect(Collectors.joining(File.separator));
  }
}
