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
import io.quarkus.logging.Log;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import java.util.HashSet;

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

  private final CCloudOAuthContext oauthContext = new CCloudOAuthContext();

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
   *     requests against the CCloud API.
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
      return oauthContext
          .checkAuthenticationStatus()
          // Consider token as invalid if any exception is thrown while we check the status
          .recover(failure -> Future.succeededFuture(false))
          .map(canAuthenticateWithConfluentCloud -> {
            // Need to re-read errors which might have been updated during the auth status check
            var errors = getAuthErrors(oauthContext);
            if (canAuthenticateWithConfluentCloud) {
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
            } else {
              // And return updated status
              return ConnectionStatusBuilder
                  .builder()
                  .ccloud(
                      new CCloudStatus(
                          ConnectedState.EXPIRED,
                          null,
                          null,
                          errors
                      )
                  ).build();
            }
          });
    }
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
