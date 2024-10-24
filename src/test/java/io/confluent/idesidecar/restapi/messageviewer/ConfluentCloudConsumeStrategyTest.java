package io.confluent.idesidecar.restapi.messageviewer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.wildfly.common.Assert.assertNotNull;
import static org.wildfly.common.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.TimestampType;
import java.io.IOException;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class ConfluentCloudConsumeStrategyTest {
  @Test
  public void testShouldParseJsonToMessageViewerData() throws IOException {
    String resourcePath = "message-viewer/ccloud-message.json";
    var json = new String(
        Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
            .getResourceAsStream(resourcePath)).readAllBytes());

    ObjectMapper objectMapper = new ObjectMapper();
    SimpleConsumeMultiPartitionResponse data = objectMapper.readValue(
        json, SimpleConsumeMultiPartitionResponse.class);

    assertNotNull(data);
    assertEquals("lkc-devcky182g", data.clusterId());
    assertEquals("dtx-test", data.topicName());

    var partitionDataList = data.partitionDataList();
    assertEquals(2, partitionDataList.size());

    var partitionData0 = partitionDataList.get(0);
    assertEquals(0, partitionData0.partitionId());
    assertEquals(1, partitionData0.nextOffset());
    assertEquals(1, partitionData0.records().size());

    var recordData0 = partitionData0.records().get(0);
    assertEquals(0, recordData0.partitionId());
    assertEquals(0, recordData0.offset());
    assertEquals(1716936949446L, recordData0.timestamp());
    assertEquals("18", recordData0.key().asText());
    assertEquals("{\"name\":\"Ravi\",\"id\":18}", recordData0.value().asText());
    assertEquals(TimestampType.CREATE_TIME, recordData0.timestampType());
    assertTrue(recordData0.headers().isEmpty());
    assertFalse(recordData0.exceededFields().key());
    assertFalse(recordData0.exceededFields().value());

    var partitionData1 = partitionDataList.get(1);
    assertEquals(1, partitionData1.partitionId());
    assertEquals(1, partitionData1.nextOffset());
    assertEquals(1, partitionData1.records().size());

    var recordData1 = partitionData1.records().get(0);
    assertEquals(1, recordData1.partitionId());
    assertEquals(0, recordData1.offset());
    assertEquals(1717001605216L, recordData1.timestamp());
    assertEquals("19", recordData1.key().asText());
    assertEquals("{\"name\":\"another-name\",\"id\":20}", recordData1.value().asText());
    assertEquals(TimestampType.CREATE_TIME, recordData1.timestampType());
    assertTrue(recordData1.headers().isEmpty());
    assertFalse(recordData1.exceededFields().key());
    assertFalse(recordData1.exceededFields().value());
  }

}
