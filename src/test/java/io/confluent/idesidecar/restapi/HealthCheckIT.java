package io.confluent.idesidecar.restapi;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.junit.jupiter.api.Tag;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
public class HealthCheckIT extends HealthCheckTest {
}