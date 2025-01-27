package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.auth.Token;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CCloudAuthProcessor extends Processor<ProxyContext, Future<ProxyContext>> {

  private final Token controlPlaneToken;

  public CCloudAuthProcessor(Token controlPlaneToken) {
    this.controlPlaneToken = controlPlaneToken;
  }

  @Override
  public Future<ProxyContext> process(ProxyContext context) {
    MultiMap headers = new HeadersMultiMap();
    headers.add("Authorization", "Bearer " + controlPlaneToken.token());
    context.setProxyRequestHeaders(headers);
    return Future.succeededFuture(context);
  }
}