package io.confluent.idesidecar.restapi.connections;

import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.TARGET_SR_CLUSTER_HEADER;

import io.confluent.idesidecar.restapi.auth.AuthErrors;
import io.confluent.idesidecar.restapi.auth.CCloudOAuthContext;
import io.confluent.idesidecar.restapi.models.ConnectionMetadata;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.CCloudStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.models.ConnectionStatusBuilder;
import io.confluent.idesidecar.restapi.resources.ConnectionsResource;
import io.netty.channel.ConnectTimeoutException;
import io.quarkus.logging.Log;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Implementation of the connection state for Confluent Cloud ({@link ConnectionType#CCLOUD}).
 */
public class CCloudConnectionState extends ConnectionState {

  static final ConnectionStatus INITIAL_STATUS = new ConnectionStatus(
      new CCloudStatus(
          ConnectedState.NONE,
          null,
          null,
          null
      ),
      null,
      null
  );

  /**
   * How long a successful {@code check_jwt} is trusted before it's sent again, while the
   * connection stays healthy. Up to {@link #CHECK_JWT_JITTER_FACTOR} more is added at random, so
   * connections don't all check at once.
   */
  static final Duration CHECK_JWT_HEALTHY_INTERVAL = Duration.ofSeconds(
      ConfigProvider.getConfig().getValue(
          "ide-sidecar.connections.ccloud.check-jwt.healthy-interval-seconds", Long.class
      ));

  static final double CHECK_JWT_JITTER_FACTOR = ConfigProvider.getConfig().getValue(
      "ide-sidecar.connections.ccloud.check-jwt.jitter-factor", Double.class
  );

  private final CCloudOAuthContext oauthContext = new CCloudOAuthContext();

  /**
   * The last {@code check_jwt} that found the connection healthy, or null.
   */
  private final AtomicReference<HealthyCheck> lastHealthyCheck = new AtomicReference<>();

  /**
   * The {@code check_jwt} running for this connection, or null.
   */
  private final AtomicReference<RunningCheck> runningCheck = new AtomicReference<>();

  CCloudConnectionState() {
    super(null, null);
  }

  CCloudConnectionState(
      @NotNull ConnectionSpec spec,
      @Nullable StateChangedListener listener
  ) {
    super(spec, listener);
  }

  @Override
  protected ConnectionStatus getInitialStatus() {
    return INITIAL_STATUS;
  }

  /**
   * For connections of type {@link ConnectionType#CCLOUD}, the status is determined as follows:
   * <ul>
   *   <li>
   *     {@link ConnectedState#NONE}, if the connection does not hold all three tokens,
   *     i.e., if the user has not yet authenticated with CCloud.
   *   </li>
   *   <li>
   *     {@link ConnectedState#SUCCESS}, if the connection holds all tokens and if it can perform
   *     API requests against the CCloud API. In this case, it also provides the time at which
   *     the user must re-authenticate with CCloud.
   *   </li>
   *   <li>
   *     {@link ConnectedState#EXPIRED}, if the connection holds all tokens but cannot perform API
   *     requests against the CCloud API. A check that times out, or can't connect in time, keeps a
   *     previous {@link ConnectedState#SUCCESS} instead, since that says nothing about the session.
   *   </li>
   *   <li>
   *     {@link ConnectedState#FAILED}, if the connection experienced a non-transient error
   *     from which it cannot recover.
   *   </li>
   * </ul>
   *
   * @return status of connection
   */
  @Override
  protected Future<ConnectionStatus> doRefreshStatus() {
    var missingTokens = new HashSet<String>();
    if (oauthContext.getRefreshToken() == null) {
      missingTokens.add("Refresh token");
    }
    if (oauthContext.getControlPlaneToken() == null) {
      missingTokens.add("Control plane token");
    }
    if (oauthContext.getDataPlaneToken() == null) {
      missingTokens.add("Data plane token");
    }

    if (!missingTokens.isEmpty()) {
      Log.infof(
          "Authentication flow for connection with ID=%s seems to be not completed because "
              + "it does not hold the following tokens: %s.",
          getId(),
          String.join(", ", missingTokens));

      return getInitialStatusWithErrors(getAuthErrors(oauthContext));
    } else if (oauthContext.hasNonTransientError()) {
      return Future.succeededFuture(
          ConnectionStatusBuilder
              .builder()
              .ccloud(
                  new CCloudStatus(
                      ConnectedState.FAILED,
                      null,
                      null,
                      getAuthErrors(oauthContext)
                  )
              )
              .build()
      );
    } else if (oauthContext.hasReachedEndOfLifetime()) {
      return Future.succeededFuture(
          ConnectionStatusBuilder
              .builder()
              .ccloud(INITIAL_STATUS.ccloud())
              .build()
      );
    } else {
      var lastStatus = getStatus();
      return checkAuthenticationStatus()
          .map(canAuthenticateWithConfluentCloud -> {
            if (!canAuthenticateWithConfluentCloud) {
              return expiredStatus();
            }
            // Need to re-read errors which might have been updated during the auth status check
            var errors = getAuthErrors(oauthContext);
            // Generate the updated status
            var user = oauthContext.getUser();
            return ConnectionStatusBuilder
                .builder()
                .ccloud(
                    new CCloudStatus(
                        ConnectedState.SUCCESS,
                        oauthContext.getEndOfLifetime(),
                        user != null ? user.asUserInfo() : null,
                        errors
                    )
                ).build();
          })
          .recover(failure -> {
            // a request that stalled, or couldn't connect in time, says nothing about the session,
            // so don't report a healthy connection as expired because of it
            if (isTimeout(failure) && isConnected(lastStatus)) {
              Log.errorf(
                  "Timed out checking the CCloud session of connection ID=%s; keeping its last "
                      + "status: %s",
                  getId(),
                  failure.getMessage());
              return Future.succeededFuture(lastStatus);
            }
            // Consider token as invalid if any other exception is thrown while we check the status
            Log.errorf(
                "Couldn't check the CCloud session of connection ID=%s: %s",
                getId(),
                failure.getMessage());
            return Future.succeededFuture(expiredStatus());
          });
    }
  }

  private ConnectionStatus expiredStatus() {
    // Need to re-read errors which might have been updated during the auth status check
    return ConnectionStatusBuilder
        .builder()
        .ccloud(
            new CCloudStatus(
                ConnectedState.EXPIRED,
                null,
                null,
                getAuthErrors(oauthContext)
            )
        ).build();
  }

  private static boolean isTimeout(Throwable failure) {
    for (var cause = failure; cause != null; cause = cause.getCause()) {
      // Netty's connect timeout is a ConnectException, but a refused connection is too, so match
      // the timeout's own type
      if (cause instanceof TimeoutException || cause instanceof ConnectTimeoutException) {
        return true;
      }
    }
    return false;
  }

  private static boolean isConnected(ConnectionStatus status) {
    return status != null
        && status.ccloud() != null
        && status.ccloud().state() == ConnectedState.SUCCESS;
  }

  /**
   * Whether the connection can make CCloud API requests. Sends {@code check_jwt} at most once per
   * healthy interval while the connection stays healthy with the same control-plane token, and
   * every time otherwise. A status refresh that comes while a check is running joins it.
   */
  private Future<Boolean> checkAuthenticationStatus() {
    var token = oauthContext.getControlPlaneToken();
    var tokenValue = token != null ? token.token() : null;
    var now = now();
    var healthy = lastHealthyCheck.get();
    if (healthy != null
        && token != null
        && Objects.equals(healthy.controlPlaneToken(), tokenValue)
        && now.isBefore(healthy.nextCheckAt())
        && token.expiresAt().isAfter(now)) {
      return Future.succeededFuture(true);
    }

    Promise<Boolean> promise = Promise.promise();
    var mine = new RunningCheck(tokenValue, promise.future());
    while (true) {
      var running = runningCheck.get();
      if (running != null && Objects.equals(running.controlPlaneToken(), tokenValue)) {
        return running.future();
      }
      // a check for an older token (say, before a new sign-in) can't answer for this one
      if (runningCheck.compareAndSet(running, mine)) {
        break;
      }
    }
    Future<Boolean> check;
    try {
      // a failure reaches every caller that joined, so each can tell a timeout from a rejection
      check = oauthContext.checkAuthenticationStatus();
    } catch (RuntimeException e) {
      runningCheck.compareAndSet(mine, null);
      Log.errorf(e, "Couldn't check the CCloud session of connection %s", getId());
      promise.complete(false);
      return promise.future();
    }
    var jitter = 1 + CHECK_JWT_JITTER_FACTOR * ThreadLocalRandom.current().nextDouble();
    check.onComplete(result -> {
      var isHealthy = result.succeeded() && Boolean.TRUE.equals(result.result());
      // counted from when the check was sent, so a slow answer doesn't stretch the interval
      lastHealthyCheck.set(isHealthy
          ? new HealthyCheck(
              tokenValue,
              now.plusMillis(Math.round(checkJwtHealthyInterval().toMillis() * jitter))
          )
          : null);
      runningCheck.compareAndSet(mine, null);
      promise.handle(result);
    });
    return promise.future();
  }

  /**
   * How long a healthy {@code check_jwt} result is reused, read through a method so tests can
   * control it.
   */
  Duration checkJwtHealthyInterval() {
    return CHECK_JWT_HEALTHY_INTERVAL;
  }

  /**
   * The current time, read through a method so tests can control it.
   */
  Instant now() {
    return Instant.now();
  }

  /**
   * A {@code check_jwt} that found the connection healthy with the given control-plane token, and
   * when to check again.
   */
  private record HealthyCheck(String controlPlaneToken, Instant nextCheckAt) {

  }

  /**
   * The {@code check_jwt} running for the given control-plane token.
   */
  private record RunningCheck(String controlPlaneToken, Future<Boolean> future) {

  }

  /**
   * Returns metadata for connections of type {@link ConnectionType#CCLOUD}. The metadata hold the
   * sign-in URI that must be opened by the extension when starting the OAuth authentication flow.
   *
   * @return connection metadata with sign-in URI
   */
  @Override
  public ConnectionMetadata getConnectionMetadata() {
    return ConnectionMetadata.from(
        oauthContext.getSignInUri(),
        ConnectionsResource.API_RESOURCE_PATH,
        spec.id());
  }

  /**
   * Return the {@link CCloudOAuthContext}'s OAuth state parameter as internal id so that the
   * callback endpoint, available at <code>/gateway/v1/callback-vscode-docs</code>, can use the
   * state parameter to look up the {@link CCloudConnectionState}.
   *
   * @return the oauthContext's state parameter
   */
  @Override
  public String getInternalId() {
    return oauthContext.getOauthState();
  }

  @Override
  public MultiMap getKafkaAuthenticationHeaders() {
    return getOauthContext().getDataPlaneAuthenticationHeaders();
  }

  @Override
  public MultiMap getSchemaRegistryAuthenticationHeaders(String clusterId) {
    var headers = HttpHeaders.headers();
    getOauthContext()
        .getDataPlaneAuthenticationHeaders()
        .forEach(headers::add);
    headers.add(TARGET_SR_CLUSTER_HEADER, clusterId);
    return headers;
  }

  public CCloudOAuthContext getOauthContext() {
    return oauthContext;
  }

  private Future<ConnectionStatus> getInitialStatusWithErrors(AuthErrors errors) {
    return Future.succeededFuture(
        ConnectionStatusBuilder
            .builder()
            .ccloud(
                new CCloudStatus(
                    ConnectedState.NONE,
                    null,
                    null,
                    errors
                )
            ).build()
    );
  }

  private AuthErrors getAuthErrors(CCloudOAuthContext authContext) {
    if (authContext.getErrors() != null && authContext.getErrors().hasErrors()) {
      return authContext.getErrors();
    } else {
      return null;
    }
  }
}
