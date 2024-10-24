package io.confluent.idesidecar.restapi.messageviewer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse.TimestampType;
import java.io.IOException;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class TestSimpleConsumeMultiPartitionResponse {

  @Test
  void testNoSchemaDataToMultiPartitionConsumeData() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    final String json1 =  new String(Objects.requireNonNull(
            Thread
                .currentThread()
                .getContextClassLoader()
                .getResourceAsStream(
                    "message-viewer/ccloud-schema-less.json")
        ).readAllBytes());
    // Deserialize JSON 1
    ConsumeResponse data1 = mapper.readValue(json1, ConsumeResponse.class);
    assertEquals("lkc-abcd123", data1.clusterId());
    assertEquals("orders_json", data1.topicName());
    ConsumeResponse.PartitionConsumeData partitionData1 = data1.partitionDataList().get(0);
    assertEquals(0, partitionData1.partitionId());
    assertEquals(60607, partitionData1.nextOffset());
    ConsumeResponse.PartitionConsumeRecord record1 = partitionData1.records().get(0);
    assertEquals(0, record1.partitionId());
    assertEquals(60606, record1.offset());
    assertEquals(1718911829088L, record1.timestamp());
    assertEquals(TimestampType.CREATE_TIME, record1.timestampType());
    assertEquals("363302", record1.key().asText());
    assertEquals("{\"ordertime\":1497402000262,\"orderid\":363302,\"itemid\":\"Item_7\",\"orderunits\":0.7202503545348564,\"address\":{\"city\":\"City_\",\"state\":\"State_35\",\"zipcode\":94086}}", record1.value().asText());

    ConsumeResponse.PartitionConsumeRecordHeader header1_1 = record1.headers().get(0);
    assertEquals("task.generation", header1_1.key());
    assertEquals("MA==", header1_1.value());
    ConsumeResponse.PartitionConsumeRecordHeader header1_2 = record1.headers().get(1);
    assertEquals("task.id", header1_2.key());
    assertEquals("MA==", header1_2.value());
    ConsumeResponse.PartitionConsumeRecordHeader header1_3 = record1.headers().get(2);
    assertEquals("current.iteration", header1_3.key());
    assertEquals("MzYzMzAy", header1_3.value());
  }


  @Test
  void testSchemaEncodedDataToMultiPartitionConsumeData() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    // Deserialize JSON 2
    final String json2 =  new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/ccloud-schema-encoded.json")
    ).readAllBytes());
    ConsumeResponse data2 = mapper.readValue(json2, ConsumeResponse.class);
    assertEquals("lkc-abcd123", data2.clusterId());
    assertEquals("order_json_sr", data2.topicName());
    ConsumeResponse.PartitionConsumeData partitionData2 = data2.partitionDataList().get(0);
    assertEquals(0, partitionData2.partitionId());
    assertEquals(60652, partitionData2.nextOffset());
    ConsumeResponse.PartitionConsumeRecord record2 = partitionData2.records().get(0);
    assertEquals(0, record2.partitionId());
    assertEquals(60602, record2.offset());
    assertEquals(1718911799411L, record2.timestamp());
    assertEquals(TimestampType.CREATE_TIME, record2.timestampType());
    assertEquals("363287", record2.key().asText());
    JsonNode value2 = record2.value();
    assertEquals("AAABhqF7Im9yZGVydGltZSI6MTQ4ODA1MjkzMjEyMywib3JkZXJpZCI6MzYzMjg3LCJpdGVtaWQiOiJJdGVtXzU0MyIsIm9yZGVydW5pdHMiOjQuNzMwMDI5ODkxOTUwODE4LCJhZGRyZXNzIjp7ImNpdHkiOiJDaXR5XyIsInN0YXRlIjoiU3RhdGVfMTgiLCJ6aXBjb2RlIjo3NTA2MH19", value2.get("__raw__").asText());

    ConsumeResponse.PartitionConsumeRecordHeader header2_1 = record2.headers().get(0);
    assertEquals("task.generation", header2_1.key());
    assertEquals("MA==", header2_1.value());
    ConsumeResponse.PartitionConsumeRecordHeader header2_2 = record2.headers().get(1);
    assertEquals("task.id", header2_2.key());
    assertEquals("MA==", header2_2.value());
    ConsumeResponse.PartitionConsumeRecordHeader header2_3 = record2.headers().get(2);
    assertEquals("current.iteration", header2_3.key());
    assertEquals("MzYzMjg3", header2_3.value());
  }
}
