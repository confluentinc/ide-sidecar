package io.confluent.idesidecar.restapi.proxy;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import jakarta.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores the context of a proxy request.
 */
public class ProxyContext {

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

  // Store the proxy response
  Buffer proxyResponseBody;
  MultiMap proxyResponseHeaders;
  int proxyResponseStatusCode;

  List<Error> errors = new ArrayList<>();

  private static final UuidFactory uuidFactory = new UuidFactory();

  public ProxyContext(String requestUri, MultiMap requestHeaders, HttpMethod requestMethod,
      Buffer requestBody, Map<String, String> requestPathParams, String connectionId) {
    this.requestUri = requestUri;
    this.requestHeaders = requestHeaders;
    this.requestMethod = requestMethod;
    this.requestBody = requestBody;
    this.requestPathParams = requestPathParams;
    this.connectionId = connectionId;
  }

  public ProxyContext error(String code, String title) {
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

  public String getConnectionId() {
    return connectionId;
  }
}
