package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@ApplicationScoped
public class FlinkPrivateEndpointUtil {

    // stores Environment ID and List of private endpoints
    private final Map<String, List<String>> flinkPrivateEndpoints = new ConcurrentHashMap<>();

    // Listen for preference changes
    public synchronized void updateFlinkPrivateEndpoints(@Observes PreferencesSpec preferences) {
        var newEndpoints = preferences.flinkPrivateEndpoints();

        flinkPrivateEndpoints.clear();
        if (newEndpoints != null && !newEndpoints.isEmpty()) {
            flinkPrivateEndpoints.putAll(newEndpoints);
            Log.infof("Updated Flink private endpoints: %s", flinkPrivateEndpoints);
        } else {
            Log.debug("Cleared Flink private endpoints (using public pattern)");
        }
    }

    /**
     * Gets private endpoints for a specified environment.
     *
     * @param environmentId the environment ID
     * @return list of private endpoints with https:// prefix, empty if none found
     */
    public List<String> getPrivateEndpoints(String environmentId) {
        if (environmentId == null) {
            return List.of();
        }

        return flinkPrivateEndpoints.getOrDefault(environmentId, List.of())
                .stream()
                .map(this::normalizeEndpointUrl)
                .toList();
    }

    private String normalizeEndpointUrl(String endpoint) {
        return endpoint.startsWith("http") ? endpoint : "https://" + endpoint;
    }

    /**
     * Validates that the endpoint is a valid Flink private endpoint URL and
     * matches the given region and provider.
     * Supports both formats:
     * - flink.{region}.{provider}.private.confluent.cloud
     * - flink.{domainid}.{region}.{provider}.private.confluent.cloud
     */
    public boolean isValidEndpointWithMatchingRegionAndProvider(String endpoint, String region, String provider) {
        if (endpoint == null || region == null || provider == null) {
            return false;
        }

        // Pattern to match both formats:
        Pattern pattern = Pattern.compile(
            "^https?://flink\\.(?:[^.]+\\.)?([^.]+)\\.([^.]+)\\.private\\.confluent\\.cloud/?$",
            Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(endpoint);
        if (!matcher.matches()) {
            return false;
        }

        String endpointRegion = matcher.group(1);
        String endpointProvider = matcher.group(2);

        return region.equalsIgnoreCase(endpointRegion) && provider.equalsIgnoreCase(endpointProvider);
    }
}
