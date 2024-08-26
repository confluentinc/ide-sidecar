package {{ packageName }};

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class {{ connectorName }} extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger({{ connectorName }}.class);

    private {{ connectorName }}Config config;

    @Override
    public void start(Map<String, String> map) {
        //TODO: Add things you need to do to setup your connector.

        config = new {{ connectorName }}Config(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        //TODO: Return your task implementation.
        return {{ connectorName }}Task.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        // Define the individual task configurations that will be executed.

        throw new UnsupportedOperationException("This has not been implemented.");
    }

    @Override
    public void stop() {
        // Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return {{ connectorName }}Config.config();
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
