package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class PredicatesTest {

  static final Predicate<String> BLANK_STRING = String::isBlank;

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

  protected void assertAlwaysTrue(Predicate<String> predicate) {
    Stream
        .of("", null, "  ", "\t\n ", "value", "longer", "us-west-2", "env-1234")
        .forEach(str -> assertTrue(predicate.test(str)));
  }

}