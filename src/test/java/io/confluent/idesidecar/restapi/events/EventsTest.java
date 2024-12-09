package io.confluent.idesidecar.restapi.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.idesidecar.restapi.events.Events.ClusterTypeQualifier;
import io.confluent.idesidecar.restapi.events.Events.ConnectionTypeQualifier;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class EventsTest {

  static final Record RECORD_1 = new Record("record 1");
  static final Record RECORD_2 = new Record("record 2");
  static final Record RECORD_3 = new Record("record 3");
  static final Record RECORD_4 = new Record("record 4");
  static final Record RECORD_5 = new Record("record 5");

  @Inject
  Event<Record> recordChannel;

  @Inject
  Consumer consumer;

  @BeforeEach
  void beforeEach() {
    consumer.clear();
  }

  @Test
  void shouldNotFireNullRecord() {
    // When a null record is fired
    fireAsyncRecord(null);

    // Then no record should be fired or detected
    consumer.assertNoMoreAsyncRecords();
    consumer.assertSyncRecords();
  }

  @Test
  void shouldHandleOneRecordWithNoQualifiersThatIsObservedByOneConsumer() {
    // When a record is fired without qualifiers
    fireAsyncRecord(RECORD_1);

    // Then only the all-records consumer will see it
    consumer.assertAllRecords(RECORD_1);

    // And there are no more async records
    consumer.assertNoMoreAsyncRecords();

    // And no sync records
    consumer.assertSyncRecords();
  }

  @Test
  void shouldHandleOneRecordThatIsObservedByOneConsumer() {
    // When a record is fired with a qualifier that no consumers are looking for
    fireAsyncRecord(RECORD_1, ClusterTypeQualifier.schemaRegistry());

    // Then only the all-records consumer will see it
    consumer.assertAllRecords(RECORD_1);

    // And there are no more async records
    consumer.assertNoMoreAsyncRecords();

    // And no sync records
    consumer.assertSyncRecords();
  }

  @Test
  void shouldHandleOneRecordThatIsObservedByMultipleConsumers() {
    // When a record is fired with qualifiers that go to all consumers
    fireAsyncRecord(RECORD_1, ClusterTypeQualifier.kafkaCluster(),
        ConnectionTypeQualifier.ccloud());

    // Then each consumer will see the record
    consumer.assertAllRecords(RECORD_1);
    consumer.assertKafkaRecords(RECORD_1);
    consumer.assertKafkaAndCCloudRecords(RECORD_1);

    // And there are no more async records
    consumer.assertNoMoreAsyncRecords();

    // And no sync records
    consumer.assertSyncRecords();
  }

  @Test
  void shouldHandleMultipleRecords() {
    // When records are fired without and with various qualifiers
    fireAsyncRecord(RECORD_1);
    fireAsyncRecord(RECORD_2, ConnectionTypeQualifier.ccloud());
    fireAsyncRecord(RECORD_3, ClusterTypeQualifier.kafkaCluster(),
        ConnectionTypeQualifier.ccloud());
    fireAsyncRecord(RECORD_4, ClusterTypeQualifier.kafkaCluster(), ConnectionTypeQualifier.local());
    fireAsyncRecord(RECORD_5, ClusterTypeQualifier.kafkaCluster());

    // Then the all-consumer should see records 1-5 in order
    consumer.assertAllRecords(RECORD_1, RECORD_2, RECORD_3, RECORD_4, RECORD_5);

    // And the kafka-consumer should see records 3-5 in order
    consumer.assertKafkaRecords(RECORD_3, RECORD_4, RECORD_5);

    // And the kafka-cloud-consumer should see record 3 only
    consumer.assertKafkaAndCCloudRecords(RECORD_3);

    // And there are no more async records
    consumer.assertNoMoreAsyncRecords();

    // And no sync records
    consumer.assertSyncRecords();
  }

  @Test
  void shouldFireSynchronousMessageThatIsObservedBySynchronousObserverOnly() {
    // When records are fired without and with various qualifiers
    fireSyncRecord(RECORD_1);
    fireSyncRecord(RECORD_2);
    fireSyncRecord(RECORD_3);
    fireSyncRecord(RECORD_4);
    fireSyncRecord(RECORD_5);

    // Then the all-consumer should see records 1-5 in order
    consumer.assertSyncRecords(RECORD_1, RECORD_2, RECORD_3, RECORD_4, RECORD_5);

    // And there are no async records
    consumer.assertNoMoreAsyncRecords();
  }

  @Test
  void shouldNotFireSynchronousNullMessage() {
    // When a null record is fired
    fireSyncRecord(null);

    // There should be no sync records
    consumer.assertSyncRecords();

    // And there are no async records
    consumer.assertNoMoreAsyncRecords();
  }

  protected void fireAsyncRecord(Record record, AnnotationLiteral<?>... qualifiers) {
    Events.fireAsyncEvent(recordChannel, record, qualifiers);
  }

  protected void fireSyncRecord(Record record) {
    Events.fireSyncEvent(recordChannel, record);
  }

  record Record(
      String msg
  ) {

  }

  @ApplicationScoped
  static class Consumer {

    final BlockingQueue<Record> kafkaRecords = new LinkedBlockingDeque<>();
    final BlockingQueue<Record> allRecords = new LinkedBlockingDeque<>();
    final BlockingQueue<Record> kafkaAndCCloud = new LinkedBlockingDeque<>();
    final List<Record> syncRecords = new ArrayList<>();

    void onSyncRecords(@Observes Record record) {
      Log.infof("sync channel received record: %s", record);
      syncRecords.add(record);
    }

    void onKafkaRecords(
        @ObservesAsync @ClusterKind.Kafka Record record
    ) {
      Log.infof("kafka channel received record: %s", record);
      kafkaRecords.add(record);
    }

    void onAllRecords(
        @ObservesAsync Record record
    ) {
      Log.infof("all channel received record: %s", record);
      allRecords.add(record);
    }

    void onKafkaAndCCloudRecords(
        @ObservesAsync @ClusterKind.Kafka @ServiceKind.CCloud Record record
    ) {
      Log.infof("kafka-and-ccloud channel received record: %s", record);
      kafkaAndCCloud.add(record);
    }

    void clear() {
      kafkaRecords.clear();
      allRecords.clear();
      kafkaAndCCloud.clear();
      syncRecords.clear();
    }

    void assertNoRecords(BlockingQueue<Record> queue, String name, Duration timeout) {
      try {
        var record = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (record != null) {
          fail("Found unexpected record in %s: %s".formatted(name, record));
        }
      } catch (InterruptedException e) {
        fail("Interrupted while waiting for records in %s".formatted(name));
      }
    }

    void assertRecordsInAnyOrder(BlockingQueue<Record> queue, String name, Record... expecteds) {
      var expectedSet = new HashSet<>(Set.of(expecteds));
      while (!expectedSet.isEmpty()) {
        try {
          var actual = queue.poll(2, TimeUnit.SECONDS);
          assertTrue(expectedSet.remove(actual));
        } catch (InterruptedException e) {
          fail("Interrupted before finding records in %s: %s".formatted(name, expectedSet));
        }
      }
    }

    void assertKafkaRecords(Record... records) {
      assertRecordsInAnyOrder(kafkaRecords, "kafka-consumer", records);
    }

    void assertAllRecords(Record... records) {
      assertRecordsInAnyOrder(allRecords, "all-consumer", records);
    }

    void assertKafkaAndCCloudRecords(Record... records) {
      assertRecordsInAnyOrder(kafkaAndCCloud, "kafka-ccloud-consumer", records);
    }

    void assertSyncRecords(Record... records) {
      assertEquals(List.of(records), syncRecords);
    }

    void assertNoMoreAsyncRecords() {
      var timeout = Duration.ofMillis(50);
      assertNoRecords(allRecords, "all-consumer", timeout);
      assertNoRecords(kafkaRecords, "kafka-consumer", timeout);
      assertNoRecords(kafkaAndCCloud, "kafka-ccloud-consumer", timeout);
    }
  }
}