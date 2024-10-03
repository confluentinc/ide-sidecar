package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.inject.Inject;
import jakarta.ws.rs.Produces;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class AdminClientService {
  @Inject
  ConnectionStateManager connectionStateManager;

  @Produces
  @RequestScoped
  public AdminClient createAdminClient(HttpServerRequest request) {
    var connectionId = request.getHeader(CONNECTION_ID_HEADER);
    // TODO: How do we use the clusterId?
    // var clusterId = request.getParam("clusterId");
    var props = connectionStateManager
        .getConnectionSpec(connectionId)
        .getAdminClientConfig();
    return AdminClient.create(props);
  }

  void closeAdminClient(@Disposes AdminClient adminClient) {
    adminClient.close();
  }
}