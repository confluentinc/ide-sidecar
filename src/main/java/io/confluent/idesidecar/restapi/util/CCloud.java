package io.confluent.idesidecar.restapi.util;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.net.URI;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Utility class for working with Confluent Cloud resources.
 */
@RegisterForReflection
public class CCloud {

  public enum Routing {
    GLB("glb"),
    PRIVATE("private"),
    INTERNAL("internal");

    private final String value;
    Routing(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    public static Routing parse(String routing) {
      for (Routing r : Routing.values()) {
        if (r.value.equalsIgnoreCase(routing)) {
          return r;
        }
      }
      throw new IllegalArgumentException("Invalid routing: " + routing);
    }
  }

  public record KafkaEndpoint(
      KafkaClusterIdentifier clusterId,
      Optional<NetworkId> networkId,
      CloudProviderRegion region,
      CloudProvider cloudProvider,
      Optional<Routing> routing,
      String domain
  ) {

    /**
     * Get the {@link URI} for the Kafka REST endpoint.
     * @return the URI
     */
    public URI getUri() {
      var builder = new StringBuilder("https://").append(clusterId);
      networkId.ifPresent(id ->
          builder.append("-").append(id)
      );
      builder.append(".").append(region);
      builder.append(".").append(cloudProvider);
      routing.ifPresent(r ->
          builder.append(".").append(r)
      );
      builder.append(".").append(domain);
      builder.append(":443");
      return URI.create(builder.toString());
    }

    /**
     * Get the bootstrap servers for the Kafka cluster.
     * @return the bootstrap servers
     */
    public String getBootstrapServers() {
      var builder = new StringBuilder().append(clusterId);
      networkId.ifPresent(id ->
          builder.append("-").append(id)
      );
      builder.append(".").append(region);
      builder.append(".").append(cloudProvider);
      routing.ifPresent(r ->
          builder.append(".").append(r)
      );
      builder.append(".").append(domain);
      builder.append(":9092");
      return builder.toString();
    }

    /**
     * Get the SchemaRegistryEndpoint given by the {@link LsrcId Schema Registry ID} and the
     * Kafka cluster endpoint.
     * @param lsrcId the Schema Registry ID
     * @return the endpoint
     */
    public SchemaRegistryEndpoint getSchemaRegistryEndpoint(LsrcId lsrcId) {
      return new SchemaRegistryEndpoint(lsrcId, region, cloudProvider, domain);
    }

    static final Pattern CCLOUD_BOOTSTRAP_SERVER_PATTERN = Pattern.compile(
        "(((pkc|lkc)-([a-z0-9]+))" // group 2: pkc or lkc
        + "([-.]([a-z0-9.]+))?" // group 6: network ID or dom slug
        + "\\.([a-z0-9-]+)" // group 7: region
        + "\\.([a-z0-9-]+)" // group 8: cloud provider
        + "\\.(([a-z0-9-]+)\\.)?" // group 10: routing
        + "((confluent|devel.cpdev|stag.cpdev)\\.cloud)):9092" // group 11: hostname
    );

    public static Optional<KafkaEndpoint> fromKafkaBootstrap(String bootstrapServers) {
      if (bootstrapServers != null) {
        // Look through the bootstrap servers and find the first one that matches the pattern
        for (var server : bootstrapServers.split(",")) {
          var matcher = CCLOUD_BOOTSTRAP_SERVER_PATTERN.matcher(server);
          if (matcher.matches()) {
            var clusterId = KafkaClusterIdentifier.parse(matcher.group(2)).orElse(null);
            var networkId = Optional.ofNullable(matcher.group(6)).map(NetworkId::new);
            var region = new CloudProviderRegion(matcher.group(7));
            var cloudProvider = new CloudProvider(matcher.group(8));
            var routing = Optional.ofNullable(matcher.group(10)).map(Routing::parse);
            var domain = matcher.group(11);
            return Optional.of(
                new KafkaEndpoint(clusterId, networkId, region, cloudProvider, routing, domain)
            );
          }
        }
      }
      return Optional.empty();
    }
  }

  public record SchemaRegistryEndpoint(
      SchemaRegistryIdentifier clusterId,
      CloudProviderRegion region,
      CloudProvider cloudProvider,
      String domain
  ) {
    public URI getUri() {
      var builder = new StringBuilder("https://").append(clusterId);
      builder.append(".").append(region);
      builder.append(".").append(cloudProvider);
      builder.append(".").append(domain);
      builder.append(":443");
      return URI.create(builder.toString());
    }

    static final Pattern CCLOUD_URI_PATTERN = Pattern.compile(
        "https://(([lp]src-([a-z0-9]+))" // group 2: pkc or lkc
        + "\\.([a-z0-9-]+)" // group 4: region
        + "\\.([a-z0-9-]+)" // group 5: cloud provider
        + "\\.(([a-z0-9-]+)\\.)?" // group 7: routing
        + "((confluent|devel.cpdev|stag.cpdev)\\.cloud)):443" // group 8: hostname
    );

    public static Optional<SchemaRegistryEndpoint> fromUri(String bootstrapServers) {
      if (bootstrapServers != null) {
        // Look through the bootstrap servers and find the first one that matches the pattern
        for (var server : bootstrapServers.split(",")) {
          var matcher = CCLOUD_URI_PATTERN.matcher(server);
          if (matcher.matches()) {
            var clusterId = SchemaRegistryIdentifier.parse(matcher.group(2)).orElse(null);
            var region = new CloudProviderRegion(matcher.group(4));
            var cloudProvider = new CloudProvider(matcher.group(5));
            // Ignore routing segment in group 7
            var domain = matcher.group(8);
            return Optional.of(
                new SchemaRegistryEndpoint(clusterId, region, cloudProvider, domain)
            );
          }
        }
      }
      return Optional.empty();
    }
  }

  public interface Identifier {
    String toString();
  }

  public interface ClusterIdentifier extends Identifier {
  }

  public interface KafkaClusterIdentifier extends ClusterIdentifier {

    static Optional<KafkaClusterIdentifier> parse(String id) {
      if (id.startsWith("pkc-")) {
        return Optional.of(new PkcId(id));
      } else if (id.startsWith("lkc-")) {
        return Optional.of(new LkcId(id));
      }
      return Optional.empty();
    }
  }

  public interface SchemaRegistryIdentifier extends ClusterIdentifier {

    static Optional<SchemaRegistryIdentifier> parse(String id) {
      if (id.startsWith("psrc-")) {
        return Optional.of(new PsrcId(id));
      } else if (id.startsWith("lsrc-")) {
        return Optional.of(new LsrcId(id));
      }
      return Optional.empty();
    }
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
  public record LkcId(String id) implements KafkaClusterIdentifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record PkcId(String id) implements KafkaClusterIdentifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record PsrcId(String id) implements SchemaRegistryIdentifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record LsrcId(String id) implements SchemaRegistryIdentifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record NetworkId(String id) implements Identifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record CloudProviderRegion(String id) implements Identifier {

    @Override
    public String toString() {
      return id;
    }
  }

  @RegisterForReflection
  public record CloudProvider(String id) implements Identifier {

    @Override
    public String toString() {
      return id;
    }
  }

}
