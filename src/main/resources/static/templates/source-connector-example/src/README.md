# Kafka Connect Source Connector

This project provides a barebones example of a Kafka Connect source connector that emits events containing incrementing numbers. It includes a unit test, provides a couple of basic configurations, and manages offsets to demonstrate how to handle task restarts gracefully. Use it as a jumping off point and massage it into a more interesting, real-world connector.

## Getting Started

### Prerequisites

* A [Confluent Cloud](https://www.confluent.io/confluent-cloud/tryfree) account
* The [Confluent CLI](https://docs.confluent.io/confluent-cli/current/install.html) installed on your machine
* Java 11 and Maven installed. This example uses Java 11 so that the connector can run in Confluent Cloud: as of the writing of this example the Confluent Cloud custom connector framework requires JDK 11. The template was last tested against OpenJDK 11.0.21 and Maven 3.9.7. If needed, you can install these prerequisites by [installing SDKMAN!](https://sdkman.io/install) and
running the following command in the root directory of this project:
```shell
sdk env
```

* We’ll build the connector with [Maven](https://maven.apache.org/index.html), so ensure that you have it [installed](https://maven.apache.org/install.html) and on your path. Before proceeding, verify that `mvn -version` succeeds when you run it on the command line.

### Installation

Run the connector's unit tests by running the following command from the directory containing `pom.xml`:
```shell
mvn test
```

You should see:
```noformat
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

To build the connector plugin zip:
```shell
mvn package
```

This generates a zip file under `target/components/packages/`.

### Usage

To run the connector in Confluent Cloud:

1. Login to Confluent Cloud:
```shell
confluent login --prompt --save
```

2. Create a temporary environment to use. This will make cleanup easier:
```shell
confluent environment create source-connector-env
```

3. Set the environment as the active environment by providing the environment ID of the form `env-123456` in the following command:
```shell
confluent environment use <ENVIRONMENT ID>
```

4. Create a Basic Kafka cluster by entering the following command, where `<provider>` is one of `aws`, `azure`, or `gcp`, and `<region>` is a region ID available in the cloud provider you choose. You can view the available regions for a given cloud provider by running `confluent kafka region list --cloud <provider>`. For example, to create a cluster in AWS region `us-east-1`:
```shell
confluent kafka cluster create quickstart --cloud aws --region us-east-1
```

5. It may take a few minutes for the cluster to be created. Validate that the cluster is running by ensuring that its `Status` is `Up` when you run the following command:
```shell
confluent kafka cluster list
```

6. Set the cluster as the active cluster by providing the cluster ID of the form `lkc-123456` in the following command:
```shell
confluent kafka cluster use <CLUSTER ID>
```

7. Upload the connector plugin to Confluent Cloud in the cloud where your cluster resides. For example, if you are using AWS:
```shell
confluent connect custom-plugin create counter-source --plugin-file target/components/packages/confluentinc-kafka-connect-counter-0.0.1-SNAPSHOT.zip --connector-type source --connector-class examples.CounterConnector --cloud aws
```
Note the ID of the form `ccp-123456` that this command outputs.

8. Now that the plugin is uploaded, we'll instantiate a connector. First create an API key. Get the ID of your cluster of the form `lkc-123456` by running `confluent kafka cluster list`. Then, create an API key that your connector will use to access it:
```shell
confluent api-key create --resource <CLUSTER ID>
```

Then, store the API key so that the CLI can use it:
```shell
confluent api-key use <API KEY>
```

9. Create the topic that will be filled by the source connector:
```shell
confluent kafka topic create counts
```

10. Create a connector configuration file `/tmp/counter-source.json` using the API key and secret generated in the previous step, as well as the plugin ID of the form `ccp-123456` output in step 4:
```json
{
  "name": "CounterSource",
  "config": {
    "connector.class": "examples.CounterConnector",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<API KEY>>",
    "kafka.api.secret": "<API SECRET>",
    "tasks.max": "1",
    "confluent.custom.plugin.id": "<PLUGIN ID>",
    "confluent.connector.type": "CUSTOM",
    "interval.ms": "1000",
    "kafka.topic": "counts"
  }
}
```

11. Provision the connector:
```shell
confluent connect cluster create --config-file /tmp/counter-source.json
```

12. Once the connector is provisioned, you will be able to consume from the `counts` topic:
```shell
confluent kafka topic consume counts --from-beginning
```
You will see an incrementing integer output every second. Enter `Ctrl+C` to exit the console consumer.

13. To clean up, delete the `source-connector-env` environment. Get your environment ID of the form `env-123456` by running `confluent environment list` and then run:
```shell
confluent environment delete <ENVIRONMENT ID>
```

## Contact

Dave Troiano - dtroiano@confluent.io
