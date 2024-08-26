# Java Consumer

This project contains a Java 21 application that subscribes to a topic on Confluent Cloud and prints
the consumed records to the console.

## Getting Started

### Prerequisites

We assume that you already have Java 21 and Gradle installed.
The template was last tested against OpenJDK 21.0.2 and Gradle 8.7.

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

You can execute the consumer application by executing:

```shell
java -cp build/libs/java-consumer-0.0.1.jar examples.ExampleConsumer
```