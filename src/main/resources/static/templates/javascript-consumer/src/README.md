# JavaScript Consumer Simple

This project contains a [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) application that consumes string values from a topic in Confluent Cloud. Of note: this project uses the promisified API similar to KafkaJS, [see the migration guide](https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/MIGRATION.md#kafkajs) for more details on the differences between it and KafkaJS. 

## Getting Started

### Prerequisites

It's assumed that you have [node and npm installed](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm). Node must be a supported version >=18.0.0 to install confluent-kafka-javascript.

Another assumption: that you have set up a cluster in the Confluent Cloud environment with a topic, a bootstrap server, and you have created an api key and secret, and that you have a way to produce data to that topic. Follow the instructions in the [Confluent documentation](https://docs.confluent.io/cloud/current/get-started/index.html) if you are unsure of how to do this. You can also use the javascript-producer template in conjunction with this one to send messages to your topic.

### Installation

To install the dependencies:

```bash
npm install
```

### Usage

Run this command to start the consumer:

```bash
node consumer.js
```

## Contact

Lucia Cerchie - lcerchie@confluent.io