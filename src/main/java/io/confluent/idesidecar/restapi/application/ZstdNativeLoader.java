package io.confluent.idesidecar.restapi.application;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
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
   * The size of the buffer used to read the native zstd-jni library file from the classpath.
   */
  private static final int BUFFER_SIZE = 8192;

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
    if (os.startsWith("win")){
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
    // Load the library file from the folder src/main/resources/libs/zstd-jni
    var libraryFile = Thread.currentThread()
        .getContextClassLoader()
        .getResource("libs/zstd-jni/%s/%s/%s".formatted(currentOs(), currentArch(), libraryName));
    // If we can't find the native library file, log an error but do not let the application
    // startup fail.
    if (libraryFile == null) {
      Log.errorf(
          "Could not find native zstd-jni library for OS=%s and Arch=%s. You probably won't be" +
              " able to consume records that were compressed with zstd.",
          currentOs(), currentArch()
      );
      return;
    }
    // Extract the library file to a temporary file
    var extractedLibFile = new File(
        System.getProperty("java.io.tmpdir"),
        libraryName
    );
    try (
        var inputStream = new BufferedInputStream(libraryFile.openStream());
        var fileOS = new FileOutputStream(extractedLibFile)
    ) {
      var data = new byte[BUFFER_SIZE];
      int byteContent;
      while ((byteContent = inputStream.read(data, 0, BUFFER_SIZE)) != -1) {
        fileOS.write(data, 0, byteContent);
      }
    } catch (IOException e) {
      Log.error("Failed to extract zstd-jni native library", e);
    }

    // Point zstd-jni to the extracted library stored in the temporary file
    System.setProperty(
        ZSTD_NATIVE_PATH_OVERRIDE,
        extractedLibFile.getAbsolutePath()
    );

    // Make sure we delete the temporary file when the application exits
    extractedLibFile.deleteOnExit();
  }
}
