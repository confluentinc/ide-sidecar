package io.confluent.idesidecar.restapi.util;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Utility class for working with Confluent Cloud resources.
 */
@RegisterForReflection
public class CCloud {

  public interface Identifier {
    String toString();
  }

  @RegisterForReflection
  public record UserId(String resourceId) implements Identifier {

    @Override
    public String toString() {
      return resourceId;
    }
  }

  @RegisterForReflection
  public record OrganizationId(String orgResourceId) implements Identifier {

    @Override
    public String toString() {
      return orgResourceId;
    }
  }

  @RegisterForReflection
  public record EnvironmentId(String envId) implements Identifier {

    @Override
    public String toString() {
      return envId;
    }
  }

  @RegisterForReflection
  public record LkcId(String id) implements Identifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record LsrcId(String id) implements Identifier {

    @Override
    public String toString() {
      return id;
    }
  }

}
