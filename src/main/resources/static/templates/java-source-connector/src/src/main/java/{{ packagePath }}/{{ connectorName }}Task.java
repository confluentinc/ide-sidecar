package {{ packageName }};

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

public class {{ connectorName }}Task extends SourceTask {

    private static Logger log = LoggerFactory.getLogger({{ connectorName }}Task.class);

    @Override
    public void start(Map<String, String> map) {
        //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        //TODO: Create SourceRecord objects that will be sent the kafka cluster.
        throw new UnsupportedOperationException("This has not been implemented.");
    }

    @Override
    public void stop() {
        //TODO: Do whatever is required to stop your task.
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
