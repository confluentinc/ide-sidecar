package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.credentials.TLSConfig;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.graph.Cluster;
import io.confluent.idesidecar.restapi.proxy.clusters.strategy.ClusterStrategy;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.net.JksOptions;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores the context of a proxy request to a cluster.
 */
public class ClusterProxyContext {

  final String clusterId;
  final ClusterType clusterType;
  Cluster clusterInfo;
  ClusterStrategy clusterStrategy;

  // Current request
  final String requestUri;
  final MultiMap requestHeaders;
  final HttpMethod requestMethod;
  final Buffer requestBody;
  final Map<String, String> requestPathParams;

  final String connectionId;
  ConnectionState connectionState;

  // Everything needed to construct the proxy request
  String proxyRequestAbsoluteUrl;
  MultiMap proxyRequestHeaders;
  HttpMethod proxyRequestMethod;
  Buffer proxyRequestBody;
  JksOptions truststoreOptions;

  // Store the proxy response
  Buffer proxyResponseBody;
  MultiMap proxyResponseHeaders;
  int proxyResponseStatusCode;

  List<Error> errors = new ArrayList<>();

  private static final UuidFactory uuidFactory = new UuidFactory();

  public ClusterProxyContext(
      String requestUri,
      MultiMap requestHeaders,
      HttpMethod requestMethod,
      Buffer requestBody,
      Map<String, String> requestPathParams,
      @Nullable String connectionId,
      String clusterId,
      ClusterType clusterType
  ) {
    this.requestUri = requestUri;
    this.requestHeaders = requestHeaders;
    this.requestMethod = requestMethod;
    this.requestBody = requestBody;
    this.requestPathParams = requestPathParams;
    this.connectionId = connectionId;
    this.clusterId = clusterId;
    this.clusterType = clusterType;
  }

  public ClusterProxyContext error(String code, String title) {
    errors.add(new Error(code, title, title, null));
    return this;
  }

  public Failure fail(int status, String message) {
    // Just set the code to generic value, change if this
    // actually serves a purpose in the future
    final String failure_code = "proxy_error";

    if (errors.isEmpty()) {
      return new Failure(
          Status.fromStatusCode(status), failure_code, message,
          uuidFactory.getRandomUuid(), List.of(new Error(failure_code, message, message, null))
      );
    } else {
      return new Failure(
          Status.fromStatusCode(status), failure_code, message,
          uuidFactory.getRandomUuid(), errors
      );
    }
  }

  public Failure failf(int status, String message, Object... args) {
    return fail(status, String.format(message, args));
  }

  // Getters and setters
  // Add additional getters and setters as needed

  public String getProxyRequestAbsoluteUrl() {
    return proxyRequestAbsoluteUrl;
  }

  public MultiMap getProxyRequestHeaders() {
    return proxyRequestHeaders;
  }

  public HttpMethod getProxyRequestMethod() {
    return proxyRequestMethod;
  }

  public Buffer getProxyRequestBody() {
    return proxyRequestBody;
  }

  public String getRequestUri() {
    return requestUri;
  }

  public HttpMethod getRequestMethod() {
    return requestMethod;
  }

  public Buffer getRequestBody() {
    return requestBody;
  }

  public MultiMap getRequestHeaders() {
    return requestHeaders;
  }

  public ConnectionState getConnectionState() {
    return connectionState;
  }

  public void setConnectionState(ConnectionState connectionState) {
    this.connectionState = connectionState;
  }


  public void setProxyRequestAbsoluteUrl(String proxyRequestAbsoluteUrl) {
    this.proxyRequestAbsoluteUrl = proxyRequestAbsoluteUrl;
  }

  public void setProxyRequestHeaders(MultiMap proxyRequestHeaders) {
    this.proxyRequestHeaders = proxyRequestHeaders;
  }

  public void setProxyRequestMethod(HttpMethod proxyRequestMethod) {
    this.proxyRequestMethod = proxyRequestMethod;
  }

  public void setProxyRequestBody(Buffer proxyRequestBody) {
    this.proxyRequestBody = proxyRequestBody;
  }

  public Buffer getProxyResponseBody() {
    return proxyResponseBody;
  }

  public void setProxyResponseBody(Buffer proxyResponseBody) {
    this.proxyResponseBody = proxyResponseBody;
  }

  public MultiMap getProxyResponseHeaders() {
    return proxyResponseHeaders;
  }

  public void setProxyResponseHeaders(MultiMap proxyResponseHeaders) {
    this.proxyResponseHeaders = proxyResponseHeaders;
  }

  public int getProxyResponseStatusCode() {
    return proxyResponseStatusCode;
  }

  public void setProxyResponseStatusCode(int proxyResponseStatusCode) {
    this.proxyResponseStatusCode = proxyResponseStatusCode;
  }

  public @Nullable String getConnectionId() {
    return connectionId;
  }

  public JksOptions getTruststoreOptions() {
    return truststoreOptions;
  }

  public void setTruststoreOptions(TLSConfig.TrustStore trustStore) {
    this.truststoreOptions = new JksOptions()
        .setPath(trustStore.path())
        .setPassword(trustStore.password().asString(false));
  }

  public Cluster getClusterInfo() {
    return clusterInfo;
  }

  public void setClusterInfo(Cluster clusterInfo) {
    this.clusterInfo = clusterInfo;
  }

  public ClusterStrategy getClusterStrategy() {
    return clusterStrategy;
  }

  public void setClusterStrategy(ClusterStrategy clusterStrategy) {
    this.clusterStrategy = clusterStrategy;
  }

  public String getClusterId() {
    return clusterId;
  }

  public ClusterType getClusterType() {
    return clusterType;
  }
}