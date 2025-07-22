package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.models.Preferences.PreferencesSpec;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class FlinkPrivateEndpointUtil {

    // stores Environment ID and List of private endpoints
    private volatile Map<String, List<String>> flinkPrivateEndpoints = Map.of();

    // Listen for preference changes
    public synchronized void updateFlinkPrivateEndpoints(@Observes PreferencesSpec preferences) {
        var newEndpoints = preferences.flinkPrivateEndpoints();

        if (newEndpoints != null && !newEndpoints.isEmpty()) {
            this.flinkPrivateEndpoints = newEndpoints;
            Log.infof("Updated Flink private endpoints: %s", flinkPrivateEndpoints);
        } else {
            this.flinkPrivateEndpoints = Map.of();
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
}
