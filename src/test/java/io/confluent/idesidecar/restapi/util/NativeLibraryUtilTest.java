package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class NativeLibraryUtilTest {

  @Test
  void extractNativeLibraryFromResourcesShouldExtractLibraryToTempFile() throws IOException {
    // Given a real file from the application's src/main/resources folder
    var temporaryFileName = "library.dylib";
    var tempFile = NativeLibraryUtil.extractNativeLibraryFromResources(
        "libs/zstd-jni/darwin/x86_64/libzstd-jni.dylib",
        temporaryFileName
    );
    // The util method should be able to extract the file to a temporary file with the provided name
    assertNotNull(tempFile);
    assertEquals(temporaryFileName, tempFile.getName());
  }

  @Test
  void extractNativeLibraryFromResourcesShouldThrowErrorIfLibraryDoesNotExist() {
    // If the provided file does not exist in the application's src/main/resources folder, expect
    // the util method to throw an IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> NativeLibraryUtil.extractNativeLibraryFromResources(
            "some/non/existing/path",
            "library.dylib"
        )
    );
  }
}
