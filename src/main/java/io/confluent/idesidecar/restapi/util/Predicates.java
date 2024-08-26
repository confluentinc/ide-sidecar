package io.confluent.idesidecar.restapi.util;

import java.util.function.Predicate;

public class Predicates {

  /**
   * Utility method that returns the supplied predicate, or an always-true predicate if the supplied
   * predicate is null.
   *
   * @param value the predicate
   * @param <T> the type of value for the predicate
   * @return the predicate if not null, or an always-true predicate
   */
  public static <T> Predicate<T> orTrue(Predicate<T> value) {
    return value != null ? value : s -> true;
  }

}
