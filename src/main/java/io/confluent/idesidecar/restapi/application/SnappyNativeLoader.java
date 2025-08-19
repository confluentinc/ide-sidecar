package io.confluent.idesidecar.restapi.application;

import io.confluent.idesidecar.restapi.util.NativeLibraryUtil;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.IOException;
import org.xerial.snappy.OSInfo;
import org.xerial.snappy.SnappyLoader;

/**
 * The {@link SnappyNativeLoader} is responsible for loading the native Snappy library included in
 * the native executable of the sidecar. Library files must be code-signed, which is why we can't
 * rely on the ones provided by the snappy-java project. Otherwise, we would run into issues at
 * runtime.
 * The {@link SnappyNativeLoader} extracts the native library from the classpath, stores it in a
 * temporary file, and passes it to the snappy-java library. After the exit of the sidecar process,
 * the temporary file will get cleaned up automatically.
 *
 * @see <a href="https://github.com/confluentinc/ide-sidecar/issues/304">ide-sidecar issue #304</a>
 */
@ApplicationScoped
public class SnappyNativeLoader {

  /**
   * This method is called during the startup of the Quarkus application to load the native Snappy
   * library for the current operating system and platform.
   * It extracts the library from the classpath and sets the necessary system properties for
   * snappy-java to use it.
   *
   * @param ev The startup event that triggers this method. Is not used.
   */
  void loadLibraryFile(@Observes StartupEvent ev) {
    // Get OS/platform-specific name of the Snappy library
    var libraryName = System.mapLibraryName("snappyjava");
    // Get path to Snappy library file for the current OS/platform
    var pathForCurrentOs = OSInfo.getNativeLibFolderPathForCurrentOS();
    try {
      var extractedLibFile = NativeLibraryUtil.extractNativeLibraryFromResources(
          "libs/snappy-java/%s/%s".formatted(pathForCurrentOs, libraryName),
          libraryName
      );
      // Point Snappy to the extracted library stored in the temporary file
      System.setProperty(
          SnappyLoader.KEY_SNAPPY_LIB_PATH,
          extractedLibFile.getParentFile().getAbsolutePath()
      );
      System.setProperty(
          SnappyLoader.KEY_SNAPPY_LIB_NAME,
          extractedLibFile.getName()
      );
    } catch (IllegalArgumentException e) {
      Log.errorf(
          "Could not find native Snappy library for OS=%s and Arch=%s. You probably won't be" +
              " able to consume records that were compressed with Snappy.",
          OSInfo.getOSName(), OSInfo.getArchName()
      );
    } catch (IOException e) {
      Log.error("Failed to extract Snappy library", e);
    }
  }
}
