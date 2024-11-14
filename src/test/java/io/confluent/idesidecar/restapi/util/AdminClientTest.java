package io.confluent.idesidecar.restapi.util;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class AdminClientTest extends AbstractSidecarIT {
    /**
     * Use the <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton Container</a>
     * pattern to ensure that the test environment is only started once, no matter how many
     * test classes extend this class. Testcontainers will assure that this is initialized once,
     * and stop the containers using the Ryuk container after all the tests have run.
     */
    private static final LocalTestEnvironment TEST_ENVIRONMENT = new LocalTestEnvironment();

    static {
        // Start up the test environment before any tests are run.
        // Let the Ryuk container handle stopping the container.
        TEST_ENVIRONMENT.start();
    }

    @Inject
    ClusterCache clusterCache;

    @BeforeEach
    public void beforeEach() {
        super.setupLocalConnection("AdminClientTest");
    }

    private static final String CLUSTER_ID = "fake-cluster-id-here";
    private static final String CONNECTION_ID = "fake-connection-id-here";

    Map<String, String> adminClientSidecarConfigs;

    private Properties getAdminClientConfig() {
        var cluster = clusterCache.getKafkaCluster(CONNECTION_ID, CLUSTER_ID);
        var props = new Properties();
        // Set AdminClient configs provided by the sidecar
        props.putAll(adminClientSidecarConfigs);
        props.put("bootstrap.servers", cluster.bootstrapServers());
        return props;
    }

    public AdminClient createdAdminClient = AdminClient.create(getAdminClientConfig());

    public String clusterId = String.valueOf(createdAdminClient.describeCluster().clusterId());


    @Test
    public void clusterHasAStringID() {
        assertInstanceOf(String.class, clusterId, "myString should be a String");
    }


}
