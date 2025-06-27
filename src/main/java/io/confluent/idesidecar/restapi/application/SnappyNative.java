package io.confluent.idesidecar.restapi.application;

import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@ApplicationScoped
public class SnappyNative {

  void initialize(@Observes StartupEvent ev) {
    var libraryFile = Thread.currentThread().getContextClassLoader()
        .getResource("libs/libsnappyjava.dylib");
    var extractedLibFile = new File(
        System.getProperty("java.io.tmpdir"),
        "libsnappyjava.dylib"
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

    // Point Snappy to the extracted library
    System.setProperty(
        "org.xerial.snappy.lib.path",
        extractedLibFile.getParentFile().getAbsolutePath()
    );
    System.setProperty(
        "org.xerial.snappy.lib.name",
        extractedLibFile.getName()
    );

    extractedLibFile.deleteOnExit();
  }
}
