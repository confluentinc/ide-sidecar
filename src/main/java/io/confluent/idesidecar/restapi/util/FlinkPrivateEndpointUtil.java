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

    // Stores Environment ID and List of private endpoints
    private final Map<String, List<String>> flinkPrivateEndpoints = new ConcurrentHashMap<>();

    /**
     * Represents a format pattern for validating and extracting region and provider
     * information from Flink private endpoint URLs.
     *
     * @param pattern       the regex pattern for the endpoint format
     * @param regionGroup   the group index for the region in the regex pattern
     * @param providerGroup the group index for the provider in the regex pattern
     */
    private record FlinkPrivateEndpointFormat(Pattern pattern, int regionGroup, int providerGroup) {}

    /** Matches flink.{region}.{provider}.private.confluent.cloud - PLATTC private networking */
    private static final Pattern PLATTC_FORMAT =
        Pattern.compile("^https?://flink\\.([^.]+)\\.([^.]+)\\.private\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE);

    /** Matches flink.{domainid}.{region}.{provider}.confluent.cloud - CCN private networking */
    private static final Pattern CCN_DOMAIN_FORMAT =
        Pattern.compile("^https?://flink\\.([^.]+)\\.([^.]+)\\.([^.]+)\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE);

    /** Matches flink-{nid}.{region}.{provider}.glb.confluent.cloud - CCN private networking */
    private static final Pattern CCN_GLB_FORMAT =
        Pattern.compile("^https?://flink-[^.]+\\.([^.]+)\\.([^.]+)\\.glb\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE);

    /** Matches flink-{peeringid}.{region}.{provider}.confluent.cloud - CCN private networking */
    private static final Pattern CCN_PEERING_FORMAT =
        Pattern.compile("^https?://flink-[^.]+\\.([^.]+)\\.([^.]+)\\.confluent\\.cloud$", Pattern.CASE_INSENSITIVE);

    // All private networking formats - both PLATTC and CCN are private networking scenarios
    private static final List<FlinkPrivateEndpointFormat> PRIVATE_FORMATS = List.of(
        new FlinkPrivateEndpointFormat(PLATTC_FORMAT, 1, 2),       // PLATTC
        new FlinkPrivateEndpointFormat(CCN_DOMAIN_FORMAT, 2, 3),   // CCN
        new FlinkPrivateEndpointFormat(CCN_GLB_FORMAT, 1, 2),      // CCN
        new FlinkPrivateEndpointFormat(CCN_PEERING_FORMAT, 1, 2)   // CCN
    );

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
     * @return list of normalized private endpoints with https:// prefix, empty if none found
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

    // Normalizes the endpoint URL by adding https:// if not present and removing trailing slash.
    private String normalizeEndpointUrl(String endpoint) {
        String normalized = endpoint.startsWith("http") ? endpoint : "https://" + endpoint;
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    // Validates that the endpoint is a valid Flink private endpoint URL and matches the given region and provider.
    public boolean isValidEndpointWithMatchingRegionAndProvider(String endpoint, String region, String provider) {
        if (endpoint == null || region == null || provider == null) {
            return false;
        }

        for (var format : PRIVATE_FORMATS) {
            Matcher matcher = format.pattern().matcher(endpoint);
            if (matcher.matches()) {
                String endpointRegion = matcher.group(format.regionGroup());
                String endpointProvider = matcher.group(format.providerGroup());
                return region.equalsIgnoreCase(endpointRegion)
                    && provider.equalsIgnoreCase(endpointProvider);
            }
        }
        return false;
    }
}
