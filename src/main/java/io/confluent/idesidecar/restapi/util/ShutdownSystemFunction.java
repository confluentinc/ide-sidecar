package io.confluent.idesidecar.restapi.util;

/**
 * Functional interface describing function to shut down Quarkus. Indirectly used to wrap
 * Quarkus.asyncExit() in {@link io.confluent.idesidecar.restapi.application.KnownWorkspacesBean}.
 * to make testing easier.
 */
@FunctionalInterface
public interface ShutdownSystemFunction {

  void shutdown();
}
