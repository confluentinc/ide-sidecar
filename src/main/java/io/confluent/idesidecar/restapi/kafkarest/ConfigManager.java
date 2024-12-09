package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;
import static java.util.Collections.singletonMap;

import io.confluent.idesidecar.restapi.clients.AdminClients;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.common.config.ConfigResource;

/**
 * RequestScoped bean for managing Kafka configurations of any {@link ConfigResource}.
 */
@RequestScoped
public class ConfigManager {

  @Inject
  AdminClients adminClients;
  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  public Uni<List<ConfigEntry>> listConfigs(String clusterId, ConfigResource resourceId) {
    return getAdminClient(clusterId)
        .chain(adminClient -> uniStage(() -> adminClient
            .describeConfigs(List.of(resourceId),
                new DescribeConfigsOptions().includeSynonyms(true))
            .all()
            .toCompletionStage())
        )
        .onItem()
        .transform(configs -> configs
            .get(resourceId)
            .entries()
            .stream()
            .toList()
        );
  }

  public Uni<Void> alterConfigs(
      String clusterId,
      ConfigResource resourceId,
      List<AlterConfigCommand> commands,
      boolean validateOnly
  ) {
    return listConfigs(clusterId, resourceId)
        .chain(configs -> ensureConfigsExist(clusterId, resourceId, commands, configs))
        .chain(ignored -> getAdminClient(clusterId))
        .chain(adminClient -> uniStage(() ->
            adminClient.incrementalAlterConfigs(
                singletonMap(
                    resourceId,
                    commands
                        .stream()
                        .map(AlterConfigCommand::toAlterConfigOp)
                        .collect(Collectors.toList())
                ),
                new AlterConfigsOptions().validateOnly(validateOnly)
            ).values().get(resourceId).toCompletionStage()));
  }

  /**
   * Ensure that all configs in the AlterConfigCommand list exist in the provided Config, which is
   * the result of a describeConfigs call.
   *
   * @param clusterId  The cluster ID (used for logging)
   * @param resourceId The resource ID (used for logging)
   * @param commands   The list of AlterConfigCommands to check existence for
   * @param configs    The Config object to check against
   * @return A Uni that completes successfully if all configs exist, or fails with a
   * NotFoundException if a config does not exist
   */
  private static Uni<Void> ensureConfigsExist(
      String clusterId,
      ConfigResource resourceId,
      List<AlterConfigCommand> commands,
      List<ConfigEntry> configs
  ) {
    final var configNames = getConfigNames(configs);

    for (AlterConfigCommand command : commands) {
      if (!configNames.contains(command.name())) {
        return Uni.createFrom().failure(
            new NotFoundException(
                String.format("Config %s cannot be found for %s %s in cluster %s.",
                    command.name(), resourceId.type(), resourceId.name(), clusterId))
        );
      }
    }

    return Uni.createFrom().voidItem();
  }

  private static Set<String> getConfigNames(List<ConfigEntry> configs) {
    return configs.stream().map(ConfigEntry::name).collect(Collectors.toSet());
  }

  private Uni<AdminClient> getAdminClient(String clusterId) {
    return uniItem(() -> adminClients.getClient(connectionId.get(), clusterId))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }
}
