package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.util.NativeLibraryUtil;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.IOException;

/**
 * The {@link ZstdNativeLoader} is responsible for loading the native zstd-jni library included in
 * the native executable of the sidecar. Library files must be code-signed, which is why we can't
 * rely on the ones provided by the zstd-jni project. Otherwise, we would run into issues at
 * runtime.
 * The {@link ZstdNativeLoader} extracts the native library from the classpath, stores it in a
 * temporary file, and passes it to the zstd-jni library. After the exit of the sidecar process,
 * the temporary file will get cleaned up automatically.
 *
 * @see <a href="https://github.com/confluentinc/ide-sidecar/issues/489">ide-sidecar issue #489</a>
 */
@ApplicationScoped
public class ZstdNativeLoader {

  /**
   * The system property that overrides the path to the native zstd-jni library.
   */
  private static final String ZSTD_NATIVE_PATH_OVERRIDE = "ZstdNativePath";

  /**
   * Determines the operating system of the user's machine using the system property <i>os.name</i>.
   *
   * @return the operating system of the user's machine
   */
  private static String currentOs() {
    var os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("win")) {
      return "win";
    } else if (os.startsWith("mac")) {
      return "darwin";
    } else {  // Linux
      return os;
    }
  }

  /**
   * Determines the architecture of the user's machine using the system property <i>os.arch</i>.
   *
   * @return the architecture of the user's machine
   */
  private static String currentArch() {
    var arch = System.getProperty("os.arch");
    if (currentOs().equals("darwin") && arch.equals("amd64")) {
      return "x86_64";
    }

    return arch;
  }

  /**
   * Determines the file extension for the native library based on the current operating system.
   *
   * @return the file extension for the native library
   */
  private static String libraryExtension() {
    if (currentOs().equals("darwin")) {
      return ".dylib";
    } else if (currentOs().equals("win")) {
      return ".dll";
    } else { // Linux
      return ".so";
    }
  }

  /**
   * This method is called during the startup of the Quarkus application to load the native zstd-jni
   * library for the current operating system and platform.
   * It extracts the library from the classpath and sets the necessary system properties for
   * zstd-jni to use it.
   *
   * @param ev The startup event that triggers this method. Is not used.
   */
  void loadLibraryFile(@Observes StartupEvent ev) {
    // Get OS/platform-specific name of the zstd-jni native library
    var libraryName = "libzstd-jni" + libraryExtension();
    try {
      var extractedLibFile = NativeLibraryUtil.extractNativeLibraryFromResources(
          "libs/zstd-jni/%s/%s/%s".formatted(currentOs(), currentArch(), libraryName),
          libraryName
      );
      // Point zstd-jni to the extracted library stored in the temporary file
      System.setProperty(
          ZSTD_NATIVE_PATH_OVERRIDE,
          extractedLibFile.getAbsolutePath()
      );
    } catch (IllegalArgumentException e) {
      Log.errorf(
          "Could not find native zstd-jni library for OS=%s and Arch=%s. You probably won't be" +
              " able to consume records that were compressed with zstd.",
          currentOs(), currentArch()
      );
    } catch (IOException e) {
      Log.error("Failed to extract zstd-jni library", e);
    }
  }
}
