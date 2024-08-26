package examples;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.file.*;

public class KafkaStreamsApplication {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApplication.class);

    private final Serde<String> stringSerde = Serdes.String();
    public static final String INPUT_TOPIC = "{{ input_topic }}";
    public static final String OUTPUT_TOPIC = "{{ output_topic }}";
    public static final Duration FIVE_SECOND_SHUTDOWN_TIMEOUT = Duration.ofSeconds(5);
    public static final int NUM_COUNTS = 1;

    public Topology buildTopology(Properties allProps) {
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> LOG.info("Observed event: {}", v))
                .mapValues(s -> s.toUpperCase())
                .peek((k, v) -> LOG.info("Transformed event: {}", v))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        return builder.build(allProps);
    }

    public static void main(String[] args) throws IOException {
        final Properties props = loadConfig("config.properties");

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-application");
        KafkaStreamsApplication kafkaStreamsApplication = new KafkaStreamsApplication();

        Topology topology = kafkaStreamsApplication.buildTopology(props);

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, props)) {
            CountDownLatch countDownLatch = new CountDownLatch(NUM_COUNTS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(FIVE_SECOND_SHUTDOWN_TIMEOUT);
                countDownLatch.countDown();
            }));
            kafkaStreams.start();
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
