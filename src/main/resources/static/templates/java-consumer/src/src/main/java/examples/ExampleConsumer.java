package examples;

import org.apache.kafka.clients.consumer.*;

import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

public class ExampleConsumer {

  public static void main(final String[] args) throws Exception {
    final String topic = "{{ cc_topic }}";

    // Load consumer configuration settings from a local file
    // Reusing the loadConfig method from the ProducerExample class
    final Properties props = loadConfig("config.properties");

    try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Arrays.asList(topic));
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          String key = record.key();
          String value = record.value();
          System.out.println(
              String.format(
                  "Consumed event from topic %s: key = %-10s value = %s",
                  topic,
                  key,
                  value));
        }
      }
    }
  }

  private static Properties loadConfig(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }
    final Properties cfg = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      cfg.load(inputStream);
    }
    return cfg;
  }

}