# Kafka Streams Simple

This project contains a Java 21 Kafka Streams application that consumes from a topic, uppercases the string values, prints them to the console, and produces the results to a Kafka topic.

## Getting Started

### Prerequisites

We assume that you already have Java 21 and Gradle installed.
The template was last tested against OpenJDK 21.0.2 and Gradle 8.7.  
You'll need to create two topics: `input` and `output` in order for this example to run.

If needed, you can install the prerequisites by [installing SDKMAN!](https://sdkman.io/install) and
running the following command in the root directory of this project:

```shell
sdk env
```

### Installation

You can compile this project by running the following command in the root directory of this project:

```shell
gradle build
```

Finally, you can build a JAR:

```shell
gradle shadowJar
```

The generated JAR can be found in the folder `build/libs`.

### Usage

You can execute the Kafka Streams application by executing:

```shell
java -jar build/libs/kafka-streams-simple-0.0.1.jar
```
