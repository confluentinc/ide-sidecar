package io.confluent.idesidecar.restapi.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public record Crn(
    String authority,
    List<Element> elements,
    boolean isRecursive
) {

  private static final String SCHEME = "crn";
  private static final String PATH_DELIMITER = "/";
  private static final String ELEMENT_JOINER = "=";
  private static final String RECURSIVE_CHAR = "*";

  @Override
  public String toString() {
    StringBuilder uri = new StringBuilder(SCHEME + "://" + authority);
    for (Element element : elements) {
      uri.append(PATH_DELIMITER).append(element.toString());
    }
    if (isRecursive) {
      uri.append(PATH_DELIMITER).append(RECURSIVE_CHAR);
    }
    return uri.toString();
  }

  public record Element(
      String resourceType,
      String resourceName
  ) {

    public Element(String resourceType, String resourceName) {
      this.resourceType = URLEncoder.encode(resourceType, StandardCharsets.UTF_8);
      this.resourceName = URLEncoder.encode(resourceName, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
      return resourceType + ELEMENT_JOINER + resourceName;
    }
  }

  public static Crn fromString(String crn) {
    URI uri;
    try {
      // Recognize crn:// scheme
      uri = new URI(crn);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid CRN: " + crn, e);
    }

    if (!SCHEME.equals(uri.getScheme())) {
      throw new IllegalArgumentException("Scheme must be " + SCHEME + " in CRN: " + crn);
    }

    if (uri.getHost() == null || uri.getHost().isEmpty()) {
      throw new IllegalArgumentException("Authority must be specified in CRN: " + crn);
    }

    String authority = uri.getHost();
    boolean isRecursive = false;
    List<Element> elements = new ArrayList<>();
    String path = uri.getRawPath().substring(1); // Remove leading "/"
    String[] pathElements = path.split(PATH_DELIMITER);

    for (int i = 0; i < pathElements.length; i++) {
      if (i == pathElements.length - 1 && pathElements[i].equals(RECURSIVE_CHAR)) {
        isRecursive = true;
        break;
      }
      String[] split = pathElements[i].split(ELEMENT_JOINER, 2);
      if (split.length != 2) {
        throw new IllegalArgumentException(
            "Element '" + pathElements[i] + "' must be 'key=value' in CRN: " + crn);
      }
      String resourceType = URLDecoder.decode(split[0], StandardCharsets.UTF_8);
      String resourceName = URLDecoder.decode(split[1], StandardCharsets.UTF_8);
      elements.add(new Element(resourceType, resourceName));
    }

    if (elements.isEmpty()) {
      throw new IllegalArgumentException("At least one element required in CRN: " + crn);
    }

    return new Crn(authority, elements, isRecursive);
  }

  public static List<Element> newElements(String... elements) {
    if (elements.length % 2 != 0) {
      throw new IllegalArgumentException("Elements must be key-value pairs");
    }
    List<Element> elementList = new ArrayList<>();
    for (int i = 0; i < elements.length; i += 2) {
      elementList.add(new Element(elements[i], elements[i + 1]));
    }
    return elementList;
  }

  protected static Crn nonRecursiveCCloud(String...elements) {
    return new Crn("confluent.cloud", newElements(elements), false);
  }

  public static Crn forOrganization(CCloud.OrganizationId orgResourceId) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString()
    );
  }

  public static Crn forEnvironment(
      CCloud.OrganizationId orgResourceId,
      CCloud.EnvironmentId envId
  ) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString(),
        "environment", envId.toString()
    );
  }

  public static Crn forKafkaCluster(
      CCloud.OrganizationId orgResourceId,
      CCloud.EnvironmentId envId,
      CCloud.LkcId lkcId
  ) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString(),
        "environment", envId.toString(),
        "cloud-cluster", lkcId.toString(),
        "kafka", lkcId.toString()
    );
  }

  public static Crn forSchemaRegistry(
      CCloud.OrganizationId orgResourceId,
      CCloud.EnvironmentId envId,
      CCloud.LsrcId lsrcId
  ) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString(),
        "environment", envId.toString(),
        "schema-registry", lsrcId.toString()
    );
  }

  public static Crn forSchemaSubject(
      CCloud.OrganizationId orgResourceId,
      CCloud.EnvironmentId envId,
      CCloud.LsrcId lsrcId,
      String subject
  ) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString(),
        "environment", envId.toString(),
        "schema-registry", lsrcId.toString(),
        "subject", subject
    );
  }

  /**
   * Create a CRN for a topic, where the topic name can be a specific name such as {@code clicks},
   * a prefix such as {@code clicks*}, or a wildcard such as {@code *}.
   *
   * @param orgResourceId            the ID of the organization
   * @param envId                    the ID of the environment
   * @param lkcId                    the ID of the LKC
   * @param topicNameOrPrefixPattern the topic name, prefix, or wildcard
   * @return the CRN
   */
  public static Crn forTopic(
      CCloud.OrganizationId orgResourceId,
      CCloud.EnvironmentId envId,
      CCloud.LkcId lkcId,
      String topicNameOrPrefixPattern
  ) {
    return nonRecursiveCCloud(
        "organization", orgResourceId.toString(),
        "environment", envId.toString(),
        "cloud-cluster", lkcId.toString(),
        "kafka", lkcId.toString(),
        "topic", topicNameOrPrefixPattern
    );
  }

}
