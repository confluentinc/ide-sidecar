package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import org.junit.jupiter.api.*;

class RecordsV3ApiImplIT {

  private static ConfluentLocalTestBed confluentLocalTestBed;
  private final static String connectionId = "local-connection";

  @Test
  void shouldReturnNotFoundWhenClusterDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenTopicDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenPartitionDoesNotExist() {

  }

  @Test
  void shouldReturnNotFoundWhenSubjectDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenSchemaVersionDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldProduceRecordWithProtobufSchemaForValue() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldProduceRecordWithProtobufSchemaForKeyAndValue() {
    // Given
    // When
    // Then
  }

  // 1. non-null Key and null value -- then we should be able to see it
  // 2. null key and null value -- handle and return 400
  // 3. null key and non-null value --
  // 4. non-null key and non-null value -- any value
  //   4.1 non-schema key and non-schema value -- (str, str), (int, str)
  //   4.2 non-schema key and schema value
  //   4.3 schema key and non-schema value
  //   4.4 schema key and schema value

  /*
  Test generator for keys
  Test generator for values
  Test generator for key and value combinations
  Test generator for schema details
   */

  // Write deser to parse values
  // Partition stuff

  @Test
  void shouldProduceRecord() {

  }
}