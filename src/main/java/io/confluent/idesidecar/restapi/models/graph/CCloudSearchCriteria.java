package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.util.Predicates.orTrue;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.function.Predicate;

/**
 * Criteria used to search for CCloud resources. Typically, a search returns only CCloud resources
 * that satisfy <i>all</i> predicates. By default, all predicates evaluate to true.
 *
 * <p>Each criteria is immutable, but can be used to clone and construct a new criteria with
 * an additional predicate. For example, the following code create a criteria where the name
 * contains the term "example":
 *
 * <pre>
 *   CCloudSearchCriteria.create()
 *                       .withNameContaining("example");
 * </pre>
 *
 * <p>Additional methods can be chained to continue adding other predicates, and the result of
 * each method is a new criteria object. For example:
 *
 * <pre>
 *   CCloudSearchCriteria.create()
 *                       .withNameContaining("example")
 *                       .withRegionContaining("us");
 * </pre>
 *
 * <p>This class does not <i>evaluate</i> the criteria. Instead, other classes like
 * {@link CCloudKafkaCluster} contain methods such as
 * {@link CCloudKafkaCluster#matches(CCloudSearchCriteria)} that determine whether the resource
 * satisfies all applicable predicates.
 *
 * @param name          the predicate for the name
 * @param environmentId the predicate for the environment ID
 * @param provider      the predicate for the {@link CloudProvider}
 * @param region        the predicate for the cloud region
 */
@RegisterForReflection
public record CCloudSearchCriteria(
    Predicate<String> name,
    Predicate<String> environmentId,
    Predicate<String> resourceId,
    Predicate<CloudProvider> provider,
    Predicate<String> region
) {

  /**
   * Create a criteria where all predicates return true. Typically, a or more of the
   * {@code with...} methods is called in a chain.
   *
   * @return a new, always true criteria object; never null
   */
  public static CCloudSearchCriteria create() {
    return new CCloudSearchCriteria(
        s -> true,
        s -> true,
        s -> true,
        s -> true,
        s -> true
    );
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * that requires the name contain the given substring, insensitive to case.
   *
   * @param substring the string that must be contained within the name
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withNameContaining(String substring) {
    var lowercase = substring.toLowerCase();
    return withName(name -> name.toLowerCase().contains(lowercase));
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * for the resource's name.
   *
   * @param predicate the new predicate for the name; if null an always-true predicate is used
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withName(Predicate<String> predicate) {
    return new CCloudSearchCriteria(orTrue(predicate), environmentId, resourceId, provider, region);
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   *that requires the environment ID contain the given substring, insensitive to case.
   *
   * @param substring the string that must be contained within the name
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withEnvironmentIdContaining(String substring) {
    var lowercase = substring.toLowerCase();
    return withEnvironmentId(envId -> envId.toLowerCase().contains(lowercase));
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * for the resource's environment ID.
   *
   * @param predicate the new predicate for the name; if null an always-true predicate is used
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withEnvironmentId(Predicate<String> predicate) {
    return new CCloudSearchCriteria(name, orTrue(predicate), resourceId, provider, region);
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * for the resource's environment ID.
   *
   * @param resourceId the ID of the resource; if null an always-true predicate is used
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withResourceId(String resourceId) {
    Predicate<String> predicate = id -> true;
    if (resourceId != null) {
      predicate = id -> id.equalsIgnoreCase(resourceId);
    }
    return new CCloudSearchCriteria(name, environmentId, predicate, provider, region);
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * that requires the name of the cloud provider contain the given substring, insensitive to case.
   *
   * @param substring the string that must be contained within the name of the cloud provider
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withCloudProviderContaining(String substring) {
    var lowercase = substring.toLowerCase();
    return withCloudProvider(csp -> csp.name().toLowerCase().contains(lowercase));
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * for the resource's cloud provider.
   *
   * @param predicate the new predicate for the name; if null an always-true predicate is used
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withCloudProvider(Predicate<CloudProvider> predicate) {
    return new CCloudSearchCriteria(name, environmentId, resourceId, orTrue(predicate), region);
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * that requires the region contain the given substring, insensitive to case.
   *
   * @param substring the string that must be contained within the name of the cloud provider
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withRegionContaining(String substring) {
    var lowercase = substring.toLowerCase();
    return withRegion(region -> region.toLowerCase().contains(lowercase));
  }

  /**
   * Return a new criteria that is a clone of this criteria object except with a new predicate
   * for the resource's cloud region.
   *
   * @param predicate the new predicate for the name; if null an always-true predicate is used
   * @return the new criteria with the new predicate; never null
   */
  public CCloudSearchCriteria withRegion(Predicate<String> predicate) {
    return new CCloudSearchCriteria(name, environmentId, resourceId, provider, orTrue(predicate));
  }
}
