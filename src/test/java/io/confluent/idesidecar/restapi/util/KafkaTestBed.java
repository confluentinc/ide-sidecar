package io.confluent.idesidecar.restapi.util;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.shaded.org.awaitility.Awaitility;

// TODO: Make this a concrete composable class
public abstract class KafkaTestBed {

  public void createTopic(String topicName, int partitions, short replicationFactor)
      throws Exception {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
      adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
    }
  }

  public void createTopic(String topicName) throws Exception {
    createTopic(topicName, 1, (short) 1);
  }

  public void deleteTopic(String topicName) throws Exception {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      adminClient.deleteTopics(Collections.singleton(topicName)).all().get(30, TimeUnit.SECONDS);
    }
  }

  public void waitForTopicCreation(String topicName, Duration timeout) {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      long pollIntervalMillis = Math.min(timeout.toMillis() / 10,
          10000); // Poll every 10 second or less, based on timeout

      Awaitility.await()
          .atMost(timeout.toMillis(), TimeUnit.MILLISECONDS)
          .pollInterval(pollIntervalMillis, TimeUnit.MILLISECONDS)
          .until(() -> adminClient.listTopics().names().get().contains(topicName));
    }
  }

  public boolean topicExists(String topicName) throws Exception{
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      return adminClient.listTopics().names().get(30, TimeUnit.SECONDS).contains(topicName);
    }
  }

  public void waitForTopicDeletion(String topicName, Duration timeout) throws Exception{
    long startTime = System.currentTimeMillis();
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
          return;
        }
        Thread.sleep(1000);
      }
      throw new TimeoutException("Timed out waiting for topic deletion: " + topicName);
    }
  }

  public Set<String> listTopics() throws Exception {
    try (AdminClient adminClient = AdminClient.create(getKafkaProperties())) {
      ListTopicsResult topics = adminClient.listTopics();
      return topics.names().get(30, TimeUnit.SECONDS);
    }
  }

  abstract protected Properties getKafkaProperties();
}
