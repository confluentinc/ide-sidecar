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
            Log.infof("Cleared Flink private endpoints (using public pattern)");
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

    /**
     * Normalizes the endpoint URL by adding https:// if not present and removing trailing slash.
     */
    private String normalizeEndpointUrl(String endpoint) {
        // Add https:// if not present
        String normalized = endpoint.startsWith("http") ? endpoint : "https://" + endpoint;

        // Remove trailing slash if present
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }

        return normalized;
    }

    /**
     * Validates that the endpoint is a valid Flink private endpoint URL and
     * matches the given region and provider.
     * Supports four formats:
     * 1. flink.{region}.{provider}.private.confluent.cloud
     * 2. flink.{domainid}.{region}.{provider}.confluent.cloud
     * 3. flink-{nid}.{region}.{provider}.glb.confluent.cloud
     * 4. flink-{peeringid}.{region}.{provider}.confluent.cloud
     */
    public boolean isValidEndpointWithMatchingRegionAndProvider(String endpoint, String region, String provider) {
        if (endpoint == null || region == null || provider == null) {
            return false;
        }

        // Define format patterns with their corresponding region/provider group indices
        var formats = new Object[][]{
            {Pattern.compile("^https?://flink\\.([^.]+)\\.([^.]+)\\.private\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE), 1, 2},
            {Pattern.compile("^https?://flink\\.([^.]+)\\.([^.]+)\\.([^.]+)\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE), 2, 3},
            {Pattern.compile("^https?://flink-[^.]+\\.([^.]+)\\.([^.]+)\\.glb\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE), 1, 2},
            {Pattern.compile("^https?://flink-[^.]+\\.([^.]+)\\.([^.]+)\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE), 1, 2},
        };

        // Try each format
        for (var format : formats) {
            Pattern pattern = (Pattern) format[0];
            int regionGroup = (int) format[1];
            int providerGroup = (int) format[2];

            Matcher matcher = pattern.matcher(endpoint);
            if (matcher.matches()) {
                String endpointRegion = matcher.group(regionGroup);
                String endpointProvider = matcher.group(providerGroup);
                return region.equalsIgnoreCase(endpointRegion) && provider.equalsIgnoreCase(endpointProvider);
            }
        }

        return false;
    }
}
