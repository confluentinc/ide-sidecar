package io.confluent.idesidecar.restapi.util;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniAndGroup2;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Helper methods for working with the Mutiny library.
 */
public final class MutinyUtil {

  private MutinyUtil() {
  }

  public static <T> Uni<T> uniStage(CompletionStage<? extends T> stage) {
    return Uni.createFrom().completionStage(stage);
  }

  public static <T> Uni<T> uniStage(Supplier<CompletionStage<? extends T>> supplier) {
    return Uni.createFrom().completionStage(supplier);
  }

  public static <T> Uni<T> uniItem(T item) {
    return Uni.createFrom().item(item);
  }

  public static <T> Uni<T> uniItem(Supplier<T> supplier) {
    return Uni.createFrom().item(supplier);
  }

  public static <T1, T2> UniAndGroup2<T1, T2> combineUnis(Uni<T1> one, Uni<T2> two) {
    return Uni.combine().all().unis(one, two);
  }

  public static <T1, T2> UniAndGroup2<T1, T2> combineUnis(Supplier<T1> one, Supplier<T2> two) {
    return combineUnis(uniItem(one), uniItem(two));
  }
}
