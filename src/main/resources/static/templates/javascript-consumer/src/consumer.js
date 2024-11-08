const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
// the consumerStart function is largely from the Confluent JavaScript client documentation: https://github.com/confluentinc/confluent-kafka-javascript/blob/dev_early_access_development_branch/QUICKSTART.md
require("dotenv").config();

const kafka = new Kafka({
    kafkaJS: {
        brokers:  ["{{ cc_bootstrap_server }}"],
        ssl: true,
        sasl: {
            mechanism: 'plain',
            username: process.env.USERNAME,
            password: process.env.PASSWORD,
        },
    }
  });
  
const consumer = kafka.consumer({'group.id':"{{ group_id }}", 'auto.offset.reset': "{{ auto_offset_reset }}"});

async function consumerStart() {

  const topic = "{{ cc_topic }}"

  await consumer.connect();
  await consumer.subscribe({ topics: [topic] });

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        offset: message.offset,
        key: message.key.toString(),
        value: message.value.toString(),
      });
    }
  });

}

consumerStart();

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})