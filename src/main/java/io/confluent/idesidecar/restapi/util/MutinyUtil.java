package io.confluent.idesidecar.restapi.util;

import io.smallrye.mutiny.Uni;
import java.util.concurrent.CompletionStage;

public final class MutinyUtil {

  private MutinyUtil() {
  }

  public static <T> Uni<T> uniStage(CompletionStage<? extends T> stage) {
    return Uni.createFrom().completionStage(stage);
  }

  public static <T> Uni<T> uniItem(T item) {
    return Uni.createFrom().item(item);
  }
}
