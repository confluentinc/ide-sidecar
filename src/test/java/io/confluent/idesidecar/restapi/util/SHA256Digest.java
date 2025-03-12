package io.confluent.idesidecar.restapi.util;

/**
 * Typed-String representation of a SHA-256 digest.
 *
 * @param digest the SHA-256 digest
 */
public record SHA256Digest(
    String digest
) {

  public String toString() {
    return digest;
  }
}
