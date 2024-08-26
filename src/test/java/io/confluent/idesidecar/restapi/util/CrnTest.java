package io.confluent.idesidecar.restapi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CrnTest {

  private static Stream<Arguments> testFromString() {
    return Stream.of(
        Arguments.of(
            "kafka topic",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks",
            new Crn(
                "confluent.cloud",
                List.of(
                    new Crn.Element("kafka", "lkc-a1b2c3"),
                    new Crn.Element("topic", "clicks")
                ),
                false
            ),
            false
        ),
        Arguments.of(
            "kafka topic prefixed",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks*",
            new Crn(
                "confluent.cloud",
                List.of(
                    new Crn.Element("kafka", "lkc-a1b2c3"),
                    new Crn.Element("topic", "clicks*")
                ),
                false
            ),
            false
        ),
        Arguments.of(
            "kafka topic wildcard",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic=*",
            new Crn(
                "confluent.cloud",
                List.of(
                    new Crn.Element("kafka", "lkc-a1b2c3"),
                    new Crn.Element("topic", "*")
                ),
                false
            ),
            false
        ),
        Arguments.of(
            "flink compute pool",
            "crn://confluent.cloud/flink-region=aws.us-east-2/compute-pool=lfcp-54mn",
            new Crn(
                "confluent.cloud",
                List.of(
                    new Crn.Element("flink-region", "aws.us-east-2"),
                    new Crn.Element("compute-pool", "lfcp-54mn")
                ),
                false
            ),
            false
        ),
        Arguments.of(
            "no authority error",
            "crn:/kafka=lkc-a1b2c3/topic=clicks",
            null,
            true
        ),
        Arguments.of(
            "malformed string 1",
            "crn://confluent.cloud/kafka:lkc-a1b2c3",
            null,
            true
        ),
        Arguments.of(
            "malformed string 2",
            "crn://confluent.cloud/kafka/topic=clicks",
            null,
            true
        ),
        Arguments.of(
            "malformed string 3",
            "crn://confluent.cloud/kafka=lkc-a1b2c3/topic",
            null,
            true
        ),
        Arguments.of(
            "not URI",
            "twas brillig and the slithy toves",
            null,
            true
        ),
        Arguments.of(
            "bad scheme",
            "http://confluent.cloud/kafka=a1b2c3",
            null,
            true
        ),
        Arguments.of(
            "kafka topic - encoded characters",
            "crn://confluent.cloud/ka%2Ff%3Dka=lkc%3Da1b%2F2c3/to%2Fp%3Dic=c%2Flic%3Dks",
            new Crn(
                "confluent.cloud",
                List.of(
                    new Crn.Element("ka/f=ka", "lkc=a1b/2c3"),
                    new Crn.Element("to/p=ic", "c/lic=ks")
                ),
                false
            ),
            false
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource
  public void testFromString(String name, String crn, Crn want,
      boolean wantErr) {
    try {
      var got = Crn.fromString(crn);
      assertEquals(want, got);
    } catch (IllegalArgumentException e) {
      if (!wantErr) {
        fail(e);
      }
    }

    // Now with the recursive flag
    try {
      var got = Crn.fromString(crn + "/*");
      assertTrue(got.isRecursive());
      assertEquals(want.elements(), got.elements());
      assertEquals(want.authority(), got.authority());
    } catch (IllegalArgumentException e) {
      if (!wantErr) {
        fail(e);
      }
    }
  }

  @Test
  void testConfluentResourceNameString() {
    var crn = new Crn(
        "confluent.cloud",
        List.of(
            new Crn.Element("kafka", "lkc-a1b2c3"),
            new Crn.Element("topic", "clicks")
        ),
        false
    );
    assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks", crn.toString());
    crn = new Crn(
        "confluent.cloud",
        List.of(
            new Crn.Element("kafka", "lkc-a1b2c3"),
            new Crn.Element("topic", "clicks")
        ),
        true
    );
    assertEquals("crn://confluent.cloud/kafka=lkc-a1b2c3/topic=clicks/*", crn.toString());
  }

  private static Stream<Arguments> testNewElement() {
    return Stream.of(
        Arguments.of(
            "good elements",
            "kafka",
            "lkc-a1b2c3",
            "kafka", "lkc-a1b2c3"
        ),
        Arguments.of(
            "bad elements 1 - encode equals in name",
            "kafka",
            "lkc=a1b2c3",
            "kafka", "lkc%3Da1b2c3"
        ),
        Arguments.of(
            "bad elements 2 - encode equals in type",
            "kaf=ka",
            "lkc-a1b2c3",
            "kaf%3Dka", "lkc-a1b2c3"
        ),
        Arguments.of(
            "bad elements 3 - encode slash in name",
            "kafka",
            "lkc/a1b2c3",
            "kafka", "lkc%2Fa1b2c3"
        ),
        Arguments.of(
            "bad elements 4 - encode slash in type",
            "kaf/ka",
            "lkc-a1b2c3",
            "kaf%2Fka", "lkc-a1b2c3"
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("testNewElement")
  void testNewElement(String name, String resourceType, String resourceName,
      String expectedResourceType, String expectedResourceName) {
    var got = new Crn.Element(resourceType, resourceName);
    assertEquals(expectedResourceName, got.resourceName());
    assertEquals(expectedResourceType, got.resourceType());
  }

  private static Stream<Arguments> testNewElements() {
    return Stream.of(
        Arguments.of(
            "kafka cluster",
            new String[]{"kafka", "lkc-a1b2c3"},
            List.of(
                new Element("kafka", "lkc-a1b2c3")
            ),
            false
        ),
        Arguments.of(
            "kafka topic",
            new String[]{"kafka", "lkc-a1b2c3", "topic", "clicks"},
            List.of(
                new Element("kafka", "lkc-a1b2c3"),
                new Element("topic", "clicks")
            ),
            false
        ),
        Arguments.of(
            "invalid parameter",
            new String[]{"kafka", "lkc/a1b2c3"},
            List.of(
                new Element("kafka", "lkc%2Fa1b2c3")
            ),
            false
        ),
        Arguments.of(
            "odd number of parameters",
            new String[]{"kafka", "lkc-a1b2c3", "wat"},
            null,
            true
        ),
        Arguments.of(
            "no resources",
            new String[]{},
            List.of(),
            false
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("testNewElements")
  void testNewElements(String name, String[] pairs, List<Element> want, boolean wantErr) {
    try {
      var got = Crn.newElements(pairs);
      for (int i = 0; i < want.size(); i++) {
        assertEquals(want.get(i).resourceName(), got.get(i).resourceName());
        assertEquals(want.get(i).resourceType(), got.get(i).resourceType());
      }
    } catch (IllegalArgumentException e) {
      if (!wantErr) {
        fail(e);
      }
    }
  }

  @Test
  void testNoElements() {
    var ex = assertThrows(IllegalArgumentException.class,
        () -> Crn.fromString("crn://confluent.cloud///"));

    assertTrue(ex.getMessage().contains("At least one element required"));
  }

  // Simple record without any encoding logic, used to perform assertions
  private record Element(
      String resourceType, String resourceName
  ) {

  }

  @Test
  void shouldCreateCrnForOrganization() {
    assertCrn(
        "crn://confluent.cloud/organization=12345",
        Crn.forOrganization(new CCloud.OrganizationId("12345"))
    );
  }

  @Test
  void shouldCreateCrnForEnvironment() {
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123",
        Crn.forEnvironment(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123")
        )
    );
  }

  @Test
  void shouldCreateCrnForLkc() {
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/cloud-cluster=lkc-abc/kafka=lkc-abc",
        Crn.forKafkaCluster(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LkcId("lkc-abc")
        )
    );
  }

  @Test
  void shouldCreateCrnForSchemaRegistry() {
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/schema-registry=lsrc-abc",
        Crn.forSchemaRegistry(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LsrcId("lsrc-abc")
        )
    );
  }

  @Test
  void shouldCreateCrnForSchemaRegistrySubject() {
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/schema-registry=lsrc-abc/subject=my-schema",
        Crn.forSchemaSubject(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LsrcId("lsrc-abc"),
            "my-schema"
        )
    );
  }

  @Test
  void shouldCreateCrnForTopicName() {
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/cloud-cluster=lkc-abc/kafka=lkc-abc/topic=clicks",
        Crn.forTopic(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LkcId("lkc-abc"),
            "clicks"
        )
    );
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/cloud-cluster=lkc-abc/kafka=lkc-abc/topic=clicks*",
        Crn.forTopic(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LkcId("lkc-abc"),
            "clicks*"
        )
    );
    assertCrn(
        "crn://confluent.cloud/organization=12345/environment=env-123/cloud-cluster=lkc-abc/kafka=lkc-abc/topic=*",
        Crn.forTopic(
            new CCloud.OrganizationId("12345"),
            new CCloud.EnvironmentId("env-123"),
            new CCloud.LkcId("lkc-abc"),
            "*"
        )
    );
  }

  protected void assertCrn(String expectedString, Crn actual) {
    assertEquals(expectedString, actual.toString());
  }
}