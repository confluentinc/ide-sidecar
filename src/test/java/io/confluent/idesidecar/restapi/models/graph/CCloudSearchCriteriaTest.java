package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CCloudSearchCriteriaTest {

  private static final Predicate<String> NOT_BLANK_STRING = Predicate.not(String::isBlank);
  private static final Predicate<CloudProvider> NONE_PROVIDER = CloudProvider.NONE::equals;

  private CCloudSearchCriteria criteria;

  @BeforeEach
  void setUp() {
    criteria = CCloudSearchCriteria.create();
  }

  @Test
  void shouldCreate() {
    assertAlwaysTrue(criteria);
  }

  @Test
  void shouldAddNamePredicate() {
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());

    criteria = criteria.withName(NOT_BLANK_STRING);
    assertSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());
  }

  @Test
  void shouldAddEnvironmentIdPredicate() {
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());

    criteria = criteria.withEnvironmentId(NOT_BLANK_STRING);
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());
  }

  @Test
  void shouldAddCloudProviderPredicate() {
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());

    criteria = criteria.withCloudProvider(NONE_PROVIDER);
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertSame(NONE_PROVIDER, criteria.provider());
  }

  @Test
  void shouldAddRegionPredicate() {
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertNotSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());

    criteria = criteria.withRegion(NOT_BLANK_STRING);
    assertNotSame(NOT_BLANK_STRING, criteria.name());
    assertNotSame(NOT_BLANK_STRING, criteria.environmentId());
    assertSame(NOT_BLANK_STRING, criteria.region());
    assertNotSame(NONE_PROVIDER, criteria.provider());
  }

  @Test
  void shouldAddNameContainsString() {
    assertPredicateWhenContainsString(
        CCloudSearchCriteria::name,
        criteria::withNameContaining,
        "value", "some-name", "other value"
    );
  }

  @Test
  void shouldAddEnvironmentIdContainsString() {
    assertPredicateWhenContainsString(
        CCloudSearchCriteria::environmentId,
        criteria::withEnvironmentIdContaining,
        "env-", "env-123"
    );
  }

  @Test
  void shouldAddRegionContainsString() {
    assertPredicateWhenContainsString(
        CCloudSearchCriteria::region,
        criteria::withRegionContaining,
        "us-", "us-west-1"
    );
  }

  @Test
  void shouldAddCloudProviderContainsString() {
    criteria = criteria.withCloudProviderContaining("AW");
    assertTrue(criteria.provider().test(CloudProvider.AWS));
    assertFalse(criteria.provider().test(CloudProvider.GCP));
    assertFalse(criteria.provider().test(CloudProvider.AZURE));
    assertFalse(criteria.provider().test(CloudProvider.NONE));

    criteria = criteria.withCloudProviderContaining("A");
    assertTrue(criteria.provider().test(CloudProvider.AWS));
    assertFalse(criteria.provider().test(CloudProvider.GCP));
    assertTrue(criteria.provider().test(CloudProvider.AZURE));
    assertFalse(criteria.provider().test(CloudProvider.NONE));

    criteria = criteria.withCloudProviderContaining("");
    assertTrue(criteria.provider().test(CloudProvider.AWS));
    assertTrue(criteria.provider().test(CloudProvider.GCP));
    assertTrue(criteria.provider().test(CloudProvider.AZURE));
    assertTrue(criteria.provider().test(CloudProvider.NONE));

    criteria = criteria.withCloudProviderContaining("NON");
    assertFalse(criteria.provider().test(CloudProvider.AWS));
    assertFalse(criteria.provider().test(CloudProvider.GCP));
    assertFalse(criteria.provider().test(CloudProvider.AZURE));
    assertTrue(criteria.provider().test(CloudProvider.NONE));

    criteria = criteria.withCloudProviderContaining("no match");
    assertFalse(criteria.provider().test(CloudProvider.AWS));
    assertFalse(criteria.provider().test(CloudProvider.GCP));
    assertFalse(criteria.provider().test(CloudProvider.AZURE));
    assertFalse(criteria.provider().test(CloudProvider.NONE));
  }

  protected <T> void assertAlwaysTrue(CCloudSearchCriteria criteria) {
    Stream
        .of("", null, "  ", "\t\n ", "value", "longer", "us-west-2", "env-1234")
        .forEach(str -> {
          assertTrue(criteria.name().test(str));
          assertTrue(criteria.environmentId().test(str));
          assertTrue(criteria.region().test(str));
          assertTrue(criteria.name().test(str));
        });
    Stream
        .of(CloudProvider.AWS, CloudProvider.GCP, CloudProvider.AZURE, CloudProvider.NONE, null)
        .forEach(csp -> {
          assertTrue(criteria.provider().test(csp));
        });
  }

  protected void assertPredicateWhenContainsString(
      Function<CCloudSearchCriteria, Predicate<String>> getter,
      Function<String, CCloudSearchCriteria> setter,
      String... substrings
  ) {
    for (String substring : substrings) {
      CCloudSearchCriteria newCriteria = setter.apply(substring);
      assertPredicateWhenContainsString(getter.apply(newCriteria), substring);
    }
  }

  protected void assertPredicateWhenContainsString(Predicate<String> p, String substring) {
    assertFalse(p.test(""));
    assertFalse(p.test("  "));
    assertFalse(p.test("\t"));
    assertFalse(p.test(substring.substring(1)));
    assertFalse(p.test(substring.charAt(0) + " " + substring.substring(1)));

    assertTrue(p.test(substring));
    assertTrue(p.test("prefix-" + substring));
    assertTrue(p.test(substring + "-suffix"));
    assertTrue(p.test("prefix-" + substring + "-suffix"));
    assertTrue(p.test(String.join(" ", substring, substring, substring)));
  }
}