package io.confluent.idesidecar.restapi.kafkarest;


import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.ws.rs.Produces;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

@ApplicationScoped
public class AdminClientService {

  // TODO: Wire this up with the ConnectionStateManager
  @Produces
  @RequestScoped
  public AdminClient createAdminClient(HttpServerRequest request) {
    var props = new Properties();
    props.put("bootstrap.servers", request.getHeader("X-bootstrap-servers"));
    // TODO: Handle case where client is not able to connect
    return AdminClient.create(props);
  }

  void closeAdminClient(@Disposes AdminClient adminClient) {
    adminClient.close();
  }
}