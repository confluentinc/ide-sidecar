package io.confluent.idesidecar.scaffolding.util;

import io.confluent.idesidecar.scaffolding.exceptions.TemplateRegistryIOException;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.FileUtils;

/**
 * Utility class for working with ZIP archives.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
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
    var tempZipFile = Files.createTempFile("templates", ".zip");
    try (var fos = new FileOutputStream(tempZipFile.toFile())) {
      fos.write(zipBytes);
    }
    FileUtils.forceDeleteOnExit(tempZipFile.toFile());

    var buffer = new byte[1024];
    var zis = new ZipInputStream(new FileInputStream(tempZipFile.toFile()));
    var zipEntry = zis.getNextEntry();
    while (zipEntry != null) {
      var newFile = newFile(outputDir.toFile(), zipEntry);
      if (zipEntry.isDirectory()) {
        if (!newFile.isDirectory() && !newFile.mkdirs()) {
          throw new IOException("Failed to create directory " + newFile);
        }
      } else {
        var parent = newFile.getParentFile();
        if (!parent.isDirectory() && !parent.mkdirs()) {
          throw new IOException("Failed to create directory " + parent);
        }

        var fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }
        fos.close();
      }
      zipEntry = zis.getNextEntry();
    }
    zis.closeEntry();
    zis.close();
  }

  public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
    var destFile = new File(destinationDir, zipEntry.getName());
    var destDirPath = destinationDir.getCanonicalPath();
    var destFilePath = destFile.getCanonicalPath();
    if (!destFilePath.startsWith(destDirPath + File.separator)) {
      // Prevent "Zip Slip" vulnerability
      throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
    }

    return destFile;
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