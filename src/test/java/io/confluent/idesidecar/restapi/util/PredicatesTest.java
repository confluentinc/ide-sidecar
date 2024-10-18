package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PredicatesTest {

  static final Predicate<String> BLANK_STRING = String::isBlank;
  static final Predicate<Integer> ALWAYS_TRUE = x -> true;
  static final Predicate<Integer> ALWAYS_FALSE = x -> false;
  static final Predicate<Integer> IS_POSITIVE = x -> x > 0;
  static final Predicate<Integer> IS_NEGATIVE = x -> x < 0;
  static final Predicate<Integer> IS_AT_LEAST_10 = x -> x >= 10;

  @Test
  void shouldReturnSamePredicateWhenNotNull() {
    assertSame(BLANK_STRING, Predicates.orTrue(BLANK_STRING));
  }

  @Test
  void shouldReturnAlwaysTruePredicateWhenParameterNull() {
    Predicate<String> result = Predicates.orTrue(null);
    assertNotNull(result);
    assertNotSame(BLANK_STRING, result);
    assertAlwaysTrue(result);
  }

  @Test
  void shouldReturnProperOrPredicateOfTwoOthers() {
    assertTrue(Predicates.or(ALWAYS_TRUE, ALWAYS_TRUE).test(1));
    assertTrue(Predicates.or(ALWAYS_TRUE, ALWAYS_FALSE).test(1));
    assertFalse(Predicates.or(ALWAYS_FALSE, ALWAYS_FALSE).test(1));

    assertFalse(Predicates.or(ALWAYS_FALSE, IS_POSITIVE).test(0));
    assertTrue(Predicates.or(ALWAYS_FALSE, IS_POSITIVE).test(1));
    assertTrue(Predicates.or(ALWAYS_TRUE, IS_POSITIVE).test(0));
    assertTrue(Predicates.or(ALWAYS_TRUE, IS_POSITIVE).test(1));
  }

  @Test
  void shouldReturnProperOrPredicateOfThreeOthers() {
    assertTrue(Predicates.or(ALWAYS_TRUE, ALWAYS_TRUE, ALWAYS_TRUE).test(1));
    assertTrue(Predicates.or(ALWAYS_TRUE, ALWAYS_FALSE, ALWAYS_FALSE).test(1));
    assertFalse(Predicates.or(ALWAYS_FALSE, ALWAYS_FALSE, ALWAYS_FALSE).test(1));

    assertFalse(Predicates.or(ALWAYS_FALSE, IS_POSITIVE, IS_AT_LEAST_10).test(0));
    assertTrue(Predicates.or(ALWAYS_FALSE, IS_POSITIVE, IS_AT_LEAST_10).test(1));
    assertFalse(Predicates.or(ALWAYS_FALSE, IS_NEGATIVE, IS_AT_LEAST_10).test(0));
    assertTrue(Predicates.or(ALWAYS_FALSE, IS_NEGATIVE, IS_AT_LEAST_10).test(10));
    assertTrue(Predicates.or(ALWAYS_TRUE, IS_POSITIVE, IS_AT_LEAST_10).test(0));
    assertTrue(Predicates.or(ALWAYS_TRUE, IS_POSITIVE, IS_AT_LEAST_10).test(1));
  }

  protected void assertAlwaysTrue(Predicate<String> predicate) {
    Stream
        .of("", null, "  ", "\t\n ", "value", "longer", "us-west-2", "env-1234")
        .forEach(str -> assertTrue(predicate.test(str)));
  }

}