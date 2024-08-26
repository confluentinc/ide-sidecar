package io.confluent.idesidecar.scaffolding.util;

import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryIOException;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;

public final class ZipUtil {

  private ZipUtil() {
  }

  /**
   * Create a zip archive from the provided content map.
   *
   * @param contentMap the map of relative file paths to file contents
   * @return the byte array of the zipped contents
   */
  public static byte[] createZipArchive(Map<String, byte[]> contentMap) {
    try (ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        BufferedOutputStream bos = new BufferedOutputStream(bytesStream);
        ZipOutputStream zos = new ZipOutputStream(bos)) {

      for (Map.Entry<String, byte[]> entry : contentMap.entrySet()) {
        // Create a new ZIP entry for each file
        ZipEntry zipEntry = new ZipEntry(entry.getKey());
        zos.putNextEntry(zipEntry);

        // Write the content bytes to the ZIP file
        zos.write(entry.getValue());

        // Close the current entry
        zos.closeEntry();
        zos.flush();
      }

      // Finish writing the ZIP file
      zos.finish();
      zos.flush();

      return bytesStream.toByteArray();
    } catch (IOException e) {
      throw new TemplateRegistryIOException(
          "Failed to create the zip archive" + e.getMessage(),
          "zip_archive_creation_failed", e);
    }
  }

  /**
   * Extract a zip archive from the provided byte array to the provided output directory.
   *
   * @throws IOException if the extraction fails
   */
  public static void extractZipFromBytes(byte[] zipBytes, Path outputDir) throws IOException {
    try (var byteArrayInputStream = new ByteArrayInputStream(zipBytes);
        var zipInputStream = new ZipInputStream(byteArrayInputStream)) {
      ZipEntry entry;
      while ((entry = zipInputStream.getNextEntry()) != null) {
        Path entryFile = outputDir.resolve(entry.getName());
        if (entry.isDirectory()) {
          Files.createDirectories(entryFile);
        } else {
          Files.createDirectories(entryFile.getParent());
          try (FileOutputStream outputStream = new FileOutputStream(entryFile.toFile())) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = zipInputStream.read(buffer)) > 0) {
              outputStream.write(buffer, 0, length);
            }
          }
        }
        zipInputStream.closeEntry();
      }
    }
  }

  /**
   * Extracts the contents from the provided ZIP to a temporary directory. Registers a JVM shutdown
   * hook to delete the temporary directory.
   *
   * @return the path to the temporary directory containing the extracted contents
   * @throws IOException if an I/O error occurs during the extraction.
   */
  public static Path extractZipContentsToTempDir(byte[] zipBytes) throws IOException {
    var tmpDir = Files.createTempDirectory(null);

    extractZipFromBytes(zipBytes, tmpDir);

    // Register a JVM shutdown hook to delete the temporary directory.
    // Known limitation: This only works if NO additional files
    // and/or folders are created inside the directory after forceDeleteOnExit was called.
    // Hence, we intentionally register this hook AFTER the ZIP extraction.
    FileUtils.forceDeleteOnExit(tmpDir.toFile());

    return tmpDir;
  }
}