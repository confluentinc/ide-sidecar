package io.confluent.idesidecar.restapi.application;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.xerial.snappy.OSInfo;

/**
 * The {@link SnappyNativeLoader} is responsible for loading the native Snappy library included in
 * the native executable of the sidecar. Library files must be code-signed, which is why we can't
 * rely on the ones provided by the snappy-java project. Otherwise,  we would run into issues at
 * runtime.
 * The {@link SnappyNativeLoader} extracts the native library from the classpath, stores it in a
 * temporary file, and passes it to the snappy-java library. After the exit of the sidecar process,
 * the temporary file will get cleaned up automatically.
 */
@ApplicationScoped
public class SnappyNativeLoader {

  void loadLibraryFile(@Observes StartupEvent ev) {
    // Get OS/platform-specific name of the Snappy library
    var libraryName = System.mapLibraryName("snappyjava");
    // Get path to Snappy library file for the current OS/platform
    var pathForCurrentOs = OSInfo.getNativeLibFolderPathForCurrentOS();
    // Load the library file from the src/main/resources folder
    var libraryFile = Thread.currentThread()
        .getContextClassLoader()
        .getResource("libs/snappy-java/%s/%s".formatted(pathForCurrentOs, libraryName));
    // If we can't find the native library file, log an error but do not let the application
    // startup fail.
    if (libraryFile == null) {
      Log.errorf(
          "Could not find native Snappy library for OS=%s and Arch=%s. You probably won't be" +
              " able to consume records that were compressed with Snappy.",
          OSInfo.getOSName(), OSInfo.getArchName()
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
      var data = new byte[8192];
      int byteContent;
      while ((byteContent = inputStream.read(data, 0, 8192)) != -1) {
        fileOS.write(data, 0, byteContent);
      }
    } catch (IOException e) {
      Log.error("Failed to extract Snappy library", e);
    }

    // Point Snappy to the extracted library stored in the temporary file
    System.setProperty(
        "org.xerial.snappy.lib.path",
        extractedLibFile.getParentFile().getAbsolutePath()
    );
    System.setProperty(
        "org.xerial.snappy.lib.name",
        extractedLibFile.getName()
    );

    // Make sure we delete the temporary file when the application exits
    extractedLibFile.deleteOnExit();
  }
}
