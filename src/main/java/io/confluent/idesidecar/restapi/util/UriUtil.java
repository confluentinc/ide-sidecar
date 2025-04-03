package io.confluent.idesidecar.restapi.util;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * Utility class for working with URIs.
 */
@ApplicationScoped
public class UriUtil {

  public static final String QUERY_PARAMETER_PREFIX = "?";

  public static final String QUERY_PARAMETER_DELIMITER_VALUE = "&";

  public static final String QUERY_PARAMETER_ENTRY_FORMAT = "%s=%s";

  private static final String INVALID_URI = "Invalid URI: ";

  /**
   * Flattens the query parameters from a UriInfo object and returns them as a String. Values of
   * query parameters are automatically encoded.
   *
   * @param uriInfo The UriInfo object.
   * @return The flattened parameters as a String.
   */
  public String getFlattenedQueryParameters(UriInfo uriInfo) {
    var flattenedParameters = uriInfo.getQueryParameters()
        .entrySet()
        .stream()
        .map(queryParameter ->
            // Query parameters might hold multiple values, so we need to flatten them, too.
            // Example: ?foo[]=bar&foo[]=baz
            queryParameter.getValue().stream()
                .map(value ->
                    String.format(
                        QUERY_PARAMETER_ENTRY_FORMAT,
                        queryParameter.getKey(),
                        encodeUri(value))
                )
                .collect(Collectors.joining(QUERY_PARAMETER_DELIMITER_VALUE))
        )
        .collect(Collectors.joining(QUERY_PARAMETER_DELIMITER_VALUE));

    return QUERY_PARAMETER_PREFIX.concat(flattenedParameters);
  }

  /**
   * Encodes a URI stored in a String.
   *
   * @param rawUri The raw URI.
   * @return The encoded URI.
   */
  public String encodeUri(String rawUri) {
    return URLEncoder.encode(
        rawUri,
        StandardCharsets.UTF_8);
  }

  public String combine(String baseUrl, String relativePath) {
    try {
      // Check if relative path is already an absolute URL
      if (relativePath.matches("^[a-zA-Z][a-zA-Z0-9+.-]*:.*")) {
        return new URI(relativePath).toString();
      }

      URI baseUri = new URI(baseUrl);

      // Handle the special case with spaces and special characters in the path
      if (relativePath.contains(" ") || containsSpecialChars(relativePath)) {
        String encodedPath = encodeCombinedPath(relativePath);

        // Build the URI manually as a string to prevent double-encoding
        StringBuilder result = new StringBuilder();
        result.append(baseUri.getScheme()).append("://").append(baseUri.getAuthority());

        if (relativePath.startsWith("/")) {
          // Absolute path relative to the host
          result.append(encodedPath);
        } else {
          // Relative to current path
          String basePath = baseUri.getPath();
          if (basePath == null || basePath.isEmpty()) {
            basePath = "/";
          } else if (!basePath.endsWith("/")) {
            basePath += "/";
          }
          result.append(basePath).append(encodedPath);
        }

        return result.toString();
      }

      // For paths with complex structures or "../"
      if (relativePath.contains("..") || (baseUri.getPath() != null && !baseUri.getPath().isEmpty() && !"/".equals(baseUri.getPath()))) {
        String basePath = baseUri.getPath();
        if (basePath == null) {
          basePath = "/";
        }

        // Handle cases where path doesn't start with /
        if (!relativePath.startsWith("/")) {
          if (!basePath.endsWith("/")) {
            basePath += "/";
          }
          relativePath = basePath + relativePath;
        } else if (!"/".equals(basePath)) {
          // For paths that start with / but have a non-root base path
          relativePath = basePath + relativePath;
        }

        // Create new URI with normalized path
        URI result = new URI(
            baseUri.getScheme(),
            baseUri.getUserInfo(),
            baseUri.getHost(),
            baseUri.getPort(),
            relativePath,
            baseUri.getQuery(),
            baseUri.getFragment()
        ).normalize();

        return result.toString();
      }

      // For regular paths
      URI relativeUri;
      if (!relativePath.startsWith("/")) {
        relativeUri = new URI("/" + relativePath);
      } else {
        relativeUri = new URI(relativePath);
      }

      URI resolvedUri = baseUri.resolve(relativeUri).normalize();
      return resolvedUri.toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + baseUrl + " or " + relativePath, e);
    }
  }

  private boolean containsSpecialChars(String path) {
    return path.matches(".*[^a-zA-Z0-9/._-].*");
  }

  private String encodeCombinedPath(String path) {
    // Simply replace spaces with %20 without any additional encoding
    return path.replace(" ", "%20");
  }

  private String encodePath(String path) {
    // Split path into segments and encode each segment separately
    String[] segments = path.split("/");
    StringBuilder encodedPath = new StringBuilder();

    for (String segment : segments) {
      if (!segment.isEmpty()) {
        // Don't encode slashes
        encodedPath.append("/").append(URLEncoder.encode(segment, StandardCharsets.UTF_8));
      }
    }

    // Handle paths that start with /
    if (path.startsWith("/") && encodedPath.length() > 0) {
      return encodedPath.toString();
    } else if (path.startsWith("/")) {
      return "/";
    } else if (encodedPath.length() > 0) {
      return encodedPath.substring(1);
    }

    return path;
  }

  public String getHost(String uri) {
    try {
      return new URI(uri).getHost();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(INVALID_URI + uri);
    }
  }

  public String getHostAndPort(String uri) {
    return getHostAndPort(uri, -1);
  }

  public String getHostAndPort(String uri, int defaultPort) {
    try {
      URI parsedUri = new URI(uri);
      return getHostAndPort(parsedUri, defaultPort);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(INVALID_URI + uri);
    }
  }

  public String getHostAndPort(URI uri) {
    return getHostAndPort(uri, -1);
  }

  public String getHostAndPort(URI uri, int defaultPort) {
    var builder = new StringBuilder();
    builder.append(uri.getHost());
    if (uri.getPort() > 0) {
      builder.append(":").append(uri.getPort());
    } else if (defaultPort > 0) {
      builder.append(":").append(defaultPort);
    }
    return builder.toString();
  }

  /**
   * Extracts the path from a URI, including the query string if present.
   */
  public String getPath(String uri) {
    try {
      URI parsedUri = new URI(uri);
      return parsedUri.getPath() + (parsedUri.getQuery() != null ? "?" + parsedUri.getQuery() : "");
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(INVALID_URI + uri);
    }
  }
}
