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
      // Create a URI from the base URL
      URI baseUri = new URI(baseUrl);

      // Create a URI from the relative path
      URI relativeUri = new URI(relativePath);

      // Resolve the relative URI against the base URI
      URI resolvedUri = baseUri.resolve(relativeUri);

      // Return the resolved URI as a string
      return resolvedUri.toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(INVALID_URI + baseUrl);
    }
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
