package io.confluent.idesidecar.restapi.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * The {@link NativeLibraryUtil} is a utility class that provides methods to extract native
 * library files from the resources folder into temporary files.
 */
public final class NativeLibraryUtil {

  /**
   * The size of the buffer used to read native library files from the classpath.
   */
  private static final int BUFFER_SIZE = 8_192;

  private NativeLibraryUtil() {
    // Utility class, no instantiation needed
  }

  /**
   * Extracts a native library file from the resources folder into a temporary file.
   *
   * @param resourcePath the path to the native library file in the resources folder
   * @param libraryName the name of the native library file to be extracted
   * @return the extracted native library file
   * @throws IOException
   * @throws IllegalArgumentException
   */
  public static File extractNativeLibraryFromResources(String resourcePath, String libraryName)
      throws IOException, IllegalArgumentException {

    // Load the library file from the folder src/main/resources
    var libraryFile = Thread.currentThread()
        .getContextClassLoader()
        .getResource(resourcePath);

    // If we can't find the native library file, throw an error
    if (libraryFile == null) {
      throw new IllegalArgumentException(
          "Could not find native library file at path: " + resourcePath
      );
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
    }

    // Make sure we delete the temporary file when the application exits
    extractedLibFile.deleteOnExit();

    return extractedLibFile;
  }

}
