package {{ packageName }};

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Map;

public class {{ connectorName }}Task extends SinkTask {

    private static Logger log = LoggerFactory.getLogger({{ connectorName }}Task.class);

    {{ connectorName }}Config config;

    @Override
    public void start(Map<String, String> settings) {
        //TODO: Create resources like database or api connections here.

        this.config = new {{ connectorName }}Config(settings);
    }

    @Override
    public void put(Collection<SinkRecord> records) {

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {
        //Close resources here.
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
