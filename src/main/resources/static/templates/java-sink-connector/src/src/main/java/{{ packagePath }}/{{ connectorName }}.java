package {{ packageName }};

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class {{ connectorName }} extends SinkConnector {

    /*
    Your connector should never use System.out for logging. All of your classes should use slf4j
    for logging
    */
    private static Logger log = LoggerFactory.getLogger({{ connectorName }}.class);

    private {{ connectorName }}Config config;

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        //TODO: Define the individual task configurations that will be executed.

        /**
         * This is used to schedule the number of tasks that will be running. This should not exceed maxTasks.
         * Here is a spot where you can dish out work. For example if you are reading from multiple tables
         * in a database, you can assign a table per task.
         */

        throw new UnsupportedOperationException("This has not been implemented.");
    }

    @Override
    public void start(Map<String, String> settings) {
        config = new {{ connectorName }}Config(settings);

        //TODO: Add things you need to do to setup your connector.

        /**
         * This will be executed once per connector. This can be used to handle connector level setup. For
         * example if you are persisting state, you can use this to method to create your state table. You
         * could also use this to verify permissions
         */
    }

    @Override
    public void stop() {
        //TODO: Do things that are necessary to stop your connector.
    }

    @Override
    public ConfigDef config() {
        return {{ connectorName }}Config.config();
    }

    @Override
    public Class<? extends Task> taskClass() {
        //TODO: Return your task implementation.
        return {{ connectorName }}Task.class;
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
