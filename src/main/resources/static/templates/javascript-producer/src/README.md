# JavaScript Producer Simple

This project contains a [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) application that produces string values to a topic in Confluent Cloud. Of note: this project uses the promisified API similar to KafkaJS, [see the migration guide](https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/MIGRATION.md#kafkajs) for more details on the differences between it and KafkaJS. 

## Getting Started

### Prerequisites

It's assumed that you have [node and npm installed](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm). Node must be a version >=18.0.0 to install confluent-kafka-javascript.

Another assumption: that you have set up a cluster in the Confluent Cloud environment with a topic, a bootstrap server, and you have created an api key and secret. Follow the instructions in the [Confluent documentation](https://docs.confluent.io/cloud/current/get-started/index.html) if you are unsure of how to do this. 

### Installation

To install the dependencies:

```bash
npm install
```

### Usage

Run this command to start the producer:

```bash
node producer.js
```

You will also likely want to add a serializer when you customize the message produced. There is some more information about how to do this using the confluent-kafka-javascript client [here](https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/INTRODUCTION.md). 

## Contact

Lucia Cerchie - lcerchie@confluent.io