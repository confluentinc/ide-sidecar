package io.confluent.idesidecar.restapi.featureflags;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.sdk.ContextKind;
import com.launchdarkly.sdk.LDContext;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.application.SidecarInfo;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.exceptions.ExceptionMappers;
import io.confluent.idesidecar.restapi.exceptions.FlagNotFoundException;
import io.confluent.idesidecar.restapi.resources.FeatureFlagsResource;
import io.confluent.idesidecar.restapi.util.Predicates;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.constraint.NotNull;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Evaluate the set of LaunchDarkly feature flags, using two LaunchDarkly {@link FeatureProject}s.
 *
 * <h2>Evaluating flags</h2>
 *
 * <p>To evaluate a feature flag, first inject a reference to {@link FeatureFlags} into the code:
 * <pre>
 * &#064;Inject
 * FeatureFlags featureFlags;
 * </pre>
 *
 * <p>Then when needed, use an {@code evaluate*(...)} method that returns the expected
 * type of value. For example, the following code gets the current value of the
 * {@link FlagId#IDE_CCLOUD_ENABLE} flag using {@link #evaluateAsBoolean(FlagId)}:
 * <pre>
 *   boolean ccloudEnabled = featureFlags.evaluateAsBoolean(FlagId.IDE_CCLOUD_ENABLE);
 * </pre>
 *
 * <p>All flags accessed with a {@link FlagId} will have default values.
 * This is enforced with unit tests.
 *
 * <h2>Adding feature flags</h2>
 *
 * <p>To add a new feature flag used within this process:
 * <ol>
 *   <li>Add a new literal to {@link FlagId}; and</li>
 *   <li>
 *     Add a default entry to {@link FlagEvaluations#DEFAULT_FLAG_EVALUATIONS_RESOURCE}
 *     (this is enforced by a unit test).
 *   </li>
 * </ol>
 *
 * <p>This ensures that the {@code evaluate*(FlagId)} methods always have a default and can safely
 * be called without worrying about a {@link FlagNotFoundException}.
 *
 * <p>Feature flags used by clients and accessed through the REST API need not have a
 * {@link FlagId}.
 * This is because the REST API will throw a {@link FlagNotFoundException} and return a
 * {@link ExceptionMappers#mapFlagNotFoundException(FlagNotFoundException) 404 Not Found error}.
 *
 * <h2>Use of LaunchDarkly</h2>
 *
 * <p>This class obtains the feature flags from LaunchDarkly for the {@link #IDE IDE} and
 * {@link #CCLOUD CCloud} projects. They are fetched immediately upon startup and will
 * {@link #refreshFlags() refresh the flags} on the
 * {@code ide-sidecar.feature-flags.poll-interval-seconds}, which by default is 60 seconds
 * for normal use or 0 seconds (no automatic refreshes) for the tests.
 *
 * <p>This class uses an HTTP client to evaluate the flags, since LaunchDarkly does not have a
 * <a href="https://docs.launchdarkly.com/sdk/concepts/client-side-server-side">Java client SDK</a>.
 * (There is a Java server-side SDK, but this is intended to be used in services
 * and is not intended to be used in client applications.)
 *
 * <p>This utility tries to limit the frequency at which the flags are evaluated.
 * The {@link #refreshFlags()} method is executed on a schedule with an interval defined by the
 * {@code ide-sidecar.feature-flags.poll-interval-seconds} application (build-time) configuration
 * property, as long as the value is positive.
 *
 * <p>There are exceptions when the flags are evaluated more frequently. When a
 * {@link #onConnectionConnected(ConnectionState) CCloud connection is first authenticated},
 * the {@link FeatureFlags} object adds a {@link #USER_CONTEXT_KIND User} context with the
 * connection's user resource ID to the {@link LDContext} used for the {@link #CCLOUD} project,
 * and immediately re-evaluates the flags. Likewise, when the
 * {@link #onConnectionDisconnected(ConnectionState) CCloud connection is disconnected}, this object
 * removes the {@link #USER_CONTEXT_KIND User} context and immediately re-evaluates the flags.
 * In this way, the feature flags always represent the current CCloud user.
 *
 * <h3>Behavior when LaunchDarkly is not available</h3>
 *
 * <p>The flag evaluations are only changed when they are successfully obtained from LaunchDarkly.
 * They flag values initially start out with <i>default</i> flags defined in
 * the {@code feature-flag-defaults.json} file. However, as soon as one or more flag evaluations are
 * successfully obtained from LaunchDarkly, the flag evaluations are cached and will continue to be
 * used until subsequent flag evaluations are obtained.
 *
 * <p>This means that if LaunchDarkly is unavailable when this object starts, the default
 * feature flags will be used. But if LaunchDarkly becomes unavailable <i>after</i> the flags
 * have been cached, the cached values will continue to be used until LaunchDarkly becomes available
 * again.
 *
 * <h3>How it works</h3>
 *
 * <p>The feature flags in the {@link #IDE IDE} project are evaluated with only a
 * {@link #DEVICE_CONTEXT_KIND Device} context that has a {@link #DEVICE_UUID device ID}
 * generated each time this process is run.
 *
 * <p>The feature flags in the {@link #CCLOUD Confluent Cloud} project are evaluated with
 * the aforementioned {@link #DEVICE_CONTEXT_KIND Device} context <i>and</i> optionally
 * a {@link #USER_CONTEXT_KIND User} context with the authenticated CCloud user's resource ID,
 * when there is an authenticated CCloud connection. When there is no authenticated Confluent Cloud
 * connection, the feature flags in the {@link #CCLOUD Confluent Cloud} project are evaluated
 * only with the {@link #DEVICE_CONTEXT_KIND Device}.
 *
 * <p>Each {@link FeatureProject} is defined with a specific
 * <a href="https://docs.launchdarkly.com/sdk/concepts/client-side-server-side#client-side-id">LaunchDarkly client-side ID</a>
 * and evaluation URL. Note that the client-side ID is <i>not</i> a secret.
 */
@Startup
@ApplicationScoped
public class FeatureFlags {

  /**
   * The LaunchDarkly {@link ContextKind} for {@code Device} contexts.
   */
  public static final ContextKind DEVICE_CONTEXT_KIND = ContextKind.of("device");

  /**
   * The LaunchDarkly {@link ContextKind} for {@code User} contexts.
   */
  public static final ContextKind USER_CONTEXT_KIND = ContextKind.of("user");

  /**
   * The generated UUID for this process. We do not have a reliable and durable device ID, and even
   * if we did we would not want to use it to avoid tracking personally identifiable information.
   */
  public static final String DEVICE_UUID = UUID.randomUUID().toString();

  private static final DateTimeFormatter HOUR_MINUTE = DateTimeFormatter
      .ofLocalizedTime(FormatStyle.MEDIUM)
      .withZone(ZoneId.systemDefault());

  /**
   * Configuration property that controls the interval between scheduled refreshes, via the
   * {@code ide-sidecar.feature-flags.refresh-interval-seconds} application property.
   * Refreshes will only be scheduled when the value is positive,
   * and the default is 0 (no automatic refreshes).
   */
  static final Duration SCHEDULED_REFRESH_INTERVAL = Duration.ofSeconds(
      ConfigProvider
          .getConfig()
          .getOptionalValue("ide-sidecar.feature-flags.refresh-interval-seconds", Long.class)
          .orElse(0L)
  );

  /**
   * The VS Code LaunchDarkly project.
   */
  public static final FeatureProject IDE = new FeatureProject(
      "IDE",
      ConfigProvider
          .getConfig()
          .getValue("ide-sidecar.feature-flags.ide-project.client-id", String.class),
      ConfigProvider
          .getConfig()
          .getValue("ide-sidecar.feature-flags.ide-project.eval-uri", String.class)
  );

  /**
   * The Confluent Cloud LaunchDarkly project.
   */
  public static final FeatureProject CCLOUD = new FeatureProject(
      "Confluent Cloud",
      ConfigProvider
          .getConfig()
          .getValue("ide-sidecar.feature-flags.ccloud-project.client-id", String.class),
      ConfigProvider
          .getConfig()
          .getValue("ide-sidecar.feature-flags.ccloud-project.eval-uri", String.class)
  );

  public static final ObjectMapper OBJECT_MAPPER = FeatureProject.OBJECT_MAPPER;

  static final CountDownLatch ZERO_LATCH = new CountDownLatch(0);

  /**
   * Create a LaunchDarkly "Device" context with a general UUID as the identifier, and use
   * this for the lifetime of the sidecar process.
   */
  private final AtomicReference<LDContext> deviceContext = new AtomicReference<>();

  /**
   * The current {@link LDContext} for the {@link #CCLOUD} project.
   */
  private final AtomicReference<LDContext> currentCCloudContext = new AtomicReference<>();

  /**
   * All LaunchDarkly projects that use the {@link #deviceContext()}.
   */
  private final List<FeatureProject> deviceProjects;

  /**
   * All LaunchDarkly projects that use the {@link #ccloudContext()}.
   */
  private final List<FeatureProject> ccloudProjects;

  private final List<FeatureProject> allProjects;

  private final MutableFlagEvaluations overrides = new MutableFlagEvaluations("overrides");
  private final MutableFlagEvaluations defaults = new MutableFlagEvaluations("defaults");

  /**
   * The next instant at which the flags can be fetched again, unless forced.
   * The initial value is 0 epoch millis, so that the flags can be fetched the first time.
   */
  private volatile Instant doNotRunUntil = Instant.ofEpochMilli(0);

  private final Duration scheduledInterval;

  @Inject
  SidecarInfo sidecar;

  @Inject
  WebClientFactory webClientFactory;

  /**
   * Create a {@link FeatureFlags} instance that uses the {@link #IDE} and {@link #CCLOUD} projects.
   */
  public FeatureFlags() {
    this(
        List.of(IDE),
        List.of(CCLOUD),
        SCHEDULED_REFRESH_INTERVAL,
        defaults -> defaults.addAll(FlagEvaluations.defaults())
    );
  }

  @VisibleForTesting
  FeatureFlags(
      List<FeatureProject> deviceProjects,
      List<FeatureProject> ccloudProjects,
      Duration scheduledInterval,
      Consumer<FlagMutations<?>> defaultInitializer
  ) {
    // Configure the projects, and make sure to initialize them with our web client factory
    this.deviceProjects = List.copyOf(deviceProjects);
    this.ccloudProjects = List.copyOf(ccloudProjects);
    this.allProjects = Stream
        .concat(this.deviceProjects.stream(), this.ccloudProjects.stream())
        .toList();
    this.scheduledInterval = scheduledInterval != null ? scheduledInterval : Duration.ofSeconds(0);

    // Build the device context and CCloud context (initially logged out)
    updateDeviceContext();
    updateCCloudContext(null);

    // Initialize the defaults
    if (defaultInitializer != null) {
      defaultInitializer.accept(defaults());
    }
  }

  /**
   * Hook to be called after this bean is initialized, after the {@link Inject @Injected} fields
   * have been set.
   */
  @PostConstruct
  void initialize() {
    // Perform the initialization, but we don't need to wait
    initializeAndWait();
  }

  @VisibleForTesting
  CountDownLatch initializeAndWait() {
    // Configure the contexts with the device information
    updateDeviceContext();
    updateCCloudContext(null);

    // Evaluate the flags for the first time
    return refreshFlags(true);
  }

  /**
   * Get all the current flags.
   *
   * @return the stream of all flags for all projects
   */
  Map<String, FlagEvaluation> evaluationsById() {
    Map<String, FlagEvaluation> combined = new HashMap<>(defaults.asMap());
    allProjects
        .stream()
        .map(FeatureProject::evaluations)
        .map(FlagEvaluations::asMap)
        .forEach(combined::putAll);
    // Then add the overrides
    combined.putAll(overrides.asMap());
    return combined;
  }

  /**
   * Get the JSON representation of the first flag with the given ID in any of the projects.
   *
   * @param id the ID of the flag to find
   * @return the current value of the flag, or empty if the flag does not exist
   */
  Optional<FlagEvaluation> findFirstFlag(@NotNull String id) {
    // First check the overrides
    var result = overrides.evaluation(id);
    if (result.isEmpty()) {
      // No luck, so look in the projects (in order)
      result = allProjects
          .stream()
          .map(FeatureProject::evaluations)
          .map(set -> set.evaluation(id).orElse(null))
          .filter(Objects::nonNull)
          .findFirst();
    }
    if (result.isEmpty()) {
      // No luck, so look at the defaults
      result = defaults.evaluation(id);
    }
    Log.debugf("Evaluating feature flag '%s' as: %s", id, result);
    return result;
  }

  /**
   * Get the JSON representation of the current value of the flag with the given ID
   * from any of the projects. This is called by the {@link FeatureFlagsResource} method(s)
   * that are used by the extension, and generally should not otherwise be used from within the
   * sidecar, which should always use the {@code evaluateAs*()} methods that take a {@link FlagId}.
   *
   * <p>This method differs from other {@code evaluateAs*()} methods.
   *
   * @param id the ID of the flag to find
   * @return the current value of the flag, or empty if the flag does not exist
   */
  public Optional<JsonNode> evaluateAsJson(String id) {
    return findFirstFlag(id).map(FlagEvaluation::value);
  }

  /**
   * Get the current value of the specified flag as a boolean, converting the value if necessary.
   *
   * <p>This will try to convert the value to a boolean.
   * JSON booleans map naturally, while integer numbers other than 0 map to true, 0 maps to false,
   * and Strings 'true' and 'false' map to corresponding values.
   *
   * <p>This method accepts a {@link FlagId}, and there should always be a
   * {@link #defaults() default value}. Therefore, this method will always return a value
   * (as verified by unit tests).
   *
   * <p>This method returns the specified {@code defaultValue} if there is no flag with the given
   * ID, or the flag's current value is unable to be converted to a boolean.
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as a boolean, or the default value
   */
  public boolean evaluateAsBoolean(@NotNull FlagId id) {
    return evaluateFlag(
        id,
        Predicates.or(JsonNode::isBoolean, JsonNode::isNumber, JsonNode::isTextual),
        JsonNode::asBoolean
    );
  }

  /**
   * Get the current value of the specified flag as an integer value, converting the value
   * if necessary.
   *
   * <p>This will try to convert the value to a Java {@link Integer}.
   * Numbers are coerced using default Java rules; booleans convert to 0 (false) and 1 (true),
   * and Strings are parsed using default Java language parsing rules for integral numbers.
   *
   * <p>This method accepts a {@link FlagId}, and there should always be a
   * {@link #defaults() default value}. Therefore, this method will always return a value
   * (as verified by unit tests).
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as an {@link Integer},
   *         which may be null if the flag value is null
   */
  public Integer evaluateAsInteger(@NotNull FlagId id) {
    return evaluateFlag(
        id,
        JsonNode::canConvertToInt,
        node -> node.isNull() ? null : node.asInt()
    );
  }

  /**
   * Get the current value of the specified flag as a long value, converting the value if necessary.
   *
   * <p>This will try to convert the value to a {@link Long}.
   * Numbers are coerced using default Java rules; booleans convert to 0 (false) and 1 (true),
   * and Strings are parsed using default Java language parsing rules for integral numbers.
   *
   * <p>This method accepts a {@link FlagId}, and there should always be a
   * {@link #defaults() default value}. Therefore, this method will always return a value
   * (as verified by unit tests).
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as a {@link Long},
   *         which may be null if the flag value is null
   */
  public Long evaluateAsLong(@NotNull FlagId id) {
    return evaluateFlag(
        id,
        JsonNode::canConvertToLong,
        node -> node.isNull() ? null : node.asLong()
    );
  }

  /**
   * Get the current string value of the specified flag, converting from the non-text value
   * if necessary.
   *
   * <p>This will try to convert the value to a string representation of the value.
   *
   * <p>This method accepts a {@link FlagId}, and there should always be a
   * {@link #defaults() default value}. Therefore, this method will always return a value
   * (as verified by unit tests).
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as a {@link String},
   *         which may be null if the flag value is null
   */
  public String evaluateAsString(@NotNull FlagId id) {
    return evaluateFlag(
        id,
        JsonNode::isValueNode,
        node -> node.isNull() ? null : node.asText()
    );
  }

  /**
   * Get the current value of the specified flag as a list of string values.
   *
   * <p>This will try to convert each value in the array to a string representation,
   * if the node is a value node (method isValueNode returns true).
   *
   * <p>This method accepts a {@link FlagId}, and there should always be a
   * {@link #defaults() default value}. Therefore, this method will always return a value
   * (as verified by unit tests).
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as a {@link String}, or the default value if the value is an
   *         array,
   */
  public List<String> evaluateAsStringList(@NotNull FlagId id) {
    return evaluateFlagAsList(
        id,
        JsonNode::textValue
    );
  }

  /**
   * Get the current value of the specified flag as a list of objects of the given type.
   *
   * <p>The JSON representation of the flag value must be an array of {@link JsonNode}s that
   * the {@link ObjectMapper} can {@link ObjectMapper#convertValue(Object, Class) convert} to
   * the specified value type.
   *
   * <p>This method returns the specified {@code defaultValue} if there is no flag with the given
   * ID, or the flag's current value is not a JSON array.
   *
   * @param id the ID of the flag to find and evaluate
   * @return the current flag value as a list of the specified value type,
   *         or the default value if the value is not an array,
   */
  public <T> List<T> evaluateAsList(@NotNull FlagId id, Class<T> valueType) {
    return evaluateFlagAsList(
        id,
        value -> OBJECT_MAPPER.convertValue(value, valueType)
    );
  }

  /**
   * Get the current value of the specified flag, by converting the raw JSON value of the feature
   * flag to the desired value type. This is a utility method used by other public.
   *
   * @param id            the ID of the boolean flag to find and evaluate
   * @param isConvertible a function to determine whether the {@link JsonNode} value can be
   *                      converted using the given {@code converter}; examples include
   *                      {@link JsonNode#canConvertToLong()} and {@link JsonNode#isTextual()}
   * @param converter     the function used to convert the {@link JsonNode} value to desired type;
   *                      examples include {@link JsonNode#asLong()} and {@link JsonNode#asText()}
   * @return the current value of the flag, or the default value
   */
  protected <T> T evaluateFlag(
      @NotNull FlagId id,
      @NotNull Predicate<JsonNode> isConvertible,
      @NotNull Function<JsonNode, T> converter
  ) {
    return findFirstFlag(id.toString())
        .map(FlagEvaluation::value)
        .filter(Objects::nonNull)
        .filter(isConvertible)
        .map(converter)
        .orElseThrow(() -> new FlagNotFoundException(id.toString()));
  }

  /**
   * Get the current value of the specified flag, by converting the raw JSON value of the feature
   * flag to a homogeneous list of the desired value type.
   * This is a utility method used by other public methods.
   *
   * @param id            the ID of the boolean flag to find and evaluate
   * @param converter     the function to convert each value of the JSON array to the desired type;
   *                      examples include {@link JsonNode#asLong()} and {@link JsonNode#asText()}
   * @return the current value of the flag as a list, or the default value
   */
  protected <T> List<T> evaluateFlagAsList(
      @NotNull FlagId id,
      @NotNull Function<JsonNode, T> converter
  ) {
    return findFirstFlag(id.toString())
        .map(FlagEvaluation::value)
        .filter(Objects::nonNull)
        .filter(JsonNode::isArray)
        .map(value -> {
          var iter = value.elements();
          List<T> results = new ArrayList<>();
          while (iter.hasNext()) {
            results.add(converter.apply(iter.next()));
          }
          return results;
        })
        .orElseThrow(() -> new FlagNotFoundException(id.toString()));
  }

  /**
   * Get the local overrides that supersede the actual feature flags. Overrides can be added
   * and removed at any time.
   *
   * @return the interface used to add, remove or clear the local overrides; never null
   */
  public FlagMutations<?> overrides() {
    return overrides;
  }

  /**
   * Get the local defaults that will be used when the corresponding feature flags cannot
   * be obtained from the configured provider(s), or if the corresponding feature flag does not
   * exist.
   *
   * @return the interface used to add, remove or clear the default flags; never null
   */
  public FlagMutations<?> defaults() {
    return defaults;
  }

  /**
   * Refresh the flags if they have not been evaluated for the configured duration.
   * This blocks until the flags have been refreshed.
   */
  @Scheduled(
      every = "${ide-sidecar.feature-flags.refresh-interval-seconds}s",
      skipExecutionIf = ScheduledRefreshPredicate.class
  )
  public void refreshFlags() {
    try {
      refreshFlags(false).await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Log.info("Interrupted while waiting for feature flag refresh");
      Thread.interrupted();
    }
  }

  /**
   * Refresh the evaluation of the LaunchDarkly feature flags using the current LD context,
   * which can change as connections are established, updated, and disconnected.
   *
   * @param forced true if the flags should be refreshed even if they were refreshed recently
   * @return the latch that can be used to wait for all projects' flags to be evaluated; never null
   */
  @VisibleForTesting
  public synchronized CountDownLatch refreshFlags(boolean forced) {
    if (forced || scheduledInterval.isPositive()) {
      // Determine the time at which this method is being called
      var now = Instant.now();

      if (forced || now.isAfter(doNotRunUntil)) {
        // We should refresh the flags
        Log.infof(
            "Checking for current feature flags in projects %s, using %d defaults, %d overrides",
            allProjects,
            defaults.size(),
            overrides.size()
        );

        // Create the latch that will be completed when all projects have refreshed
        var latch = new CountDownLatch(deviceProjects.size() + ccloudProjects.size());

        // Evaluate the project(s) using the device context.
        // The cached evaluations are updated asynchronously.
        deviceProjects.forEach(
            project -> project.refresh(deviceContext(), webClientFactory, latch::countDown)
        );

        // Evaluate the project(s) using the CCloud context.
        // The cached evaluations are updated asynchronously.
        ccloudProjects.forEach(
            project -> project.refresh(ccloudContext(), webClientFactory, latch::countDown)
        );

        // Set the next minimum time the flags CAN be run, using "almost" the interval
        doNotRunUntil = now.plus(scheduledInterval);
        if (scheduledInterval.isPositive()) {
          Log.infof(
              "Next evaluation of feature flags will be after %s (in %ss)",
              HOUR_MINUTE.format(doNotRunUntil),
              scheduledInterval.getSeconds()
          );
        }

        // And finally return the latch that will complete when the refreshes do
        return latch;
      } else {
        Log.infof(
            "Skipping evaluation of feature flags until %s",
            HOUR_MINUTE.format(doNotRunUntil)
        );
      }
    }
    return ZERO_LATCH;
  }

  /**
   * Respond to the connection being disconnected by updating the LD context
   * and re-evaluating the flags.
   *
   * <p>This updates the LaunchDarkly context used to evaluate feature flags to contain:
   * <ol>
   *   <li>
   *     the general {@code Device} context with the {@link #DEVICE_UUID generated device UUID}; and
   *   </li>
   *   <li>
   *     a {@code User} context with the logged in CCloud {@code userResourceId} as the context key
   *   </li>
   * </ol>
   *
   * @param connection the connection that was disconnected
   */
  void onConnectionConnected(@ObservesAsync @Lifecycle.Connected ConnectionState connection) {
    if (connection instanceof CCloudConnectionState ccloudConnection) {
      updateCCloudContext(ccloudConnection);
    }
    refreshFlags(true);
  }

  /**
   * Respond to the connection being updated by updating the LD context
   * and re-evaluating the flags.
   *
   * <p>This updates the LaunchDarkly context used to evaluate feature flags to contain:
   * <ol>
   *   <li>
   *     the general {@code Device} context with the {@link #DEVICE_UUID generated device UUID}; and
   *   </li>
   *   <li>
   *     a {@code User} context with the logged in CCloud {@code userResourceId} as the context key
   *   </li>
   * </ol>
   *
   * @param connection the connection that was dupdated
   */
  void onConnectionUpdated(@ObservesAsync @Lifecycle.Connected ConnectionState connection) {
    if (connection instanceof CCloudConnectionState ccloudConnection) {
      updateCCloudContext(ccloudConnection);
    }
    refreshFlags(true);
  }

  /**
   * Respond to the connection being disconnected by updating the LD context
   * and re-evaluating the flags.
   *
   * <p>This updates the LaunchDarkly context used to evaluate feature flags to contain only:
   * <ol>
   *   <li>
   *     the general {@code Device} context with the {@link #DEVICE_UUID generated device UUID}; and
   *   </li>
   * </ol>
   *
   * @param connection the connection that was disconnected
   */
  void onConnectionDisconnected(@ObservesAsync @Lifecycle.Disconnected ConnectionState connection) {
    // Set the context to the general context only
    updateCCloudContext(null);
    refreshFlags(true);
  }

  /**
   * Utility to construct the {@link LDContext} for the given CCloud connection.
   *
   * @param state the CCloud connection state
   * @return the LaunchDarkly context
   */
  void updateCCloudContext(CCloudConnectionState state) {
    LDContext ccloudContext;
    if (state != null) {
      // The user is logged in, so build a new context with that information
      var oauthContext = state.getOauthContext();
      var userDetails = oauthContext.getUser();
      var orgDetails = oauthContext.getCurrentOrganization();
      var userContext = LDContext
          .builder(USER_CONTEXT_KIND, userDetails.resourceId())
          .key(userDetails.resourceId())
          .set("user.resource_id", userDetails.resourceId())
          .set("org.resource_id", orgDetails.resourceId())
          .set("email", userDetails.email())
          .build();
      // And add the device context
      ccloudContext = LDContext.createMulti(
          deviceContext.get(),
          userContext
      );
    } else {
      // Otherwise just use the device context for CCloud
      ccloudContext = deviceContext.get();
    }
    currentCCloudContext.set(ccloudContext);
  }

  void updateDeviceContext() {
    var context = LDContext
        .builder(DEVICE_CONTEXT_KIND, DEVICE_UUID)
        .set("id", DEVICE_UUID);
    if (sidecar != null) {
      context
          .set("sidecar.version", sidecar.version())
          .set("os.version", sidecar.osVersion())
          .set("os.name", sidecar.osName())
          .set("os.type", sidecar.osType().name());
      sidecar.vsCode().ifPresent(vscode -> context
          .set("vscode.extension.version", vscode.extensionVersion())
          .set("vscode.version", vscode.version())
          .set("vscode.uri.scheme", vscode.uriScheme()));
    }
    this.deviceContext.set(context.build());
  }

  @VisibleForTesting
  LDContext deviceContext() {
    return this.deviceContext.get();
  }

  @VisibleForTesting
  LDContext ccloudContext() {
    return this.currentCCloudContext.get();
  }
}
