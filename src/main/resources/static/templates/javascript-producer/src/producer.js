const { Kafka } = require("@confluentinc/kafka-javascript").KafkaJS;
require("dotenv").config();

const brokers = ["{{ cc_bootstrap_server }}"];

const kafka = new Kafka({
  kafkaJS: {
    clientId: "{{ client_id }}",
    brokers: brokers,
    connectionTimeout: 10000,
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: process.env.USERNAME,
      password: process.env.PASSWORD,
    },
  },
});

const producer = kafka.producer();

// producer should always be connected at app initialization, separately from producing message
const connectProducer = async () => {
  await producer.connect();
  console.log("Connected successfully");
};

const run = async () => {
  try {
    let i = 0;

    while (true) {
      let messageValue = {
        messageFieldKey: `messageFieldValue${i}`,
      };

      let messageKey = `messageKey${i}`

      console.log({messageKey, messageValue});

      producer.send({
        topic: "{{ cc_topic }}",
        messages: [{ key: JSON.stringify(messageKey), value: JSON.stringify(messageValue) }],
      });
      i++;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  } catch (error) {
    console.error("Error thrown during send:", error);
  } finally {
    await producer.disconnect();
  }
  console.log("Disconnected successfully");
};

(async () => {
  await connectProducer();
  await run();
})();

const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
