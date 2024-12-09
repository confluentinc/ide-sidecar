package io.confluent.idesidecar.restapi.util;

import io.smallrye.common.constraint.NotNull;
import java.util.function.Predicate;

public class Predicates {

  /**
   * Utility method that returns the supplied predicate, or an always-true predicate if the supplied
   * predicate is null.
   *
   * @param value the predicate
   * @param <T>   the type of value for the predicate
   * @return the predicate if not null, or an always-true predicate
   */
  public static <T> Predicate<T> orTrue(Predicate<T> value) {
    return value != null ? value : s -> true;
  }

  /**
   * Utility method that returns a predicate function that evaluates to true if either of the
   * supplied predicates evaluates to true.
   *
   * @param first  the first predicate to evaluate
   * @param second the second predicate to evaluate
   * @param <T>    the type of predicate parameter
   * @return the new predicate function
   */
  public static <T> Predicate<T> or(
      @NotNull Predicate<T> first,
      @NotNull Predicate<T> second
  ) {
    return (val) -> first.test(val) || second.test(val);
  }

  /**
   * Utility method that returns a predicate function that evaluates to true if any of the supplied
   * predicates evaluates to true.
   *
   * @param first  the first predicate to evaluate
   * @param second the second predicate to evaluate
   * @param third  the third predicate to evaluate
   * @param <T>    the type of predicate parameter
   * @return the new predicate function
   */
  public static <T> Predicate<T> or(
      @NotNull Predicate<T> first,
      @NotNull Predicate<T> second,
      @NotNull Predicate<T> third
  ) {
    return (val) -> first.test(val) || second.test(val) || third.test(val);
  }

}
