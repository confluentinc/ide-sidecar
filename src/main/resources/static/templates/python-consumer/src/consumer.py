#!/usr/bin/env python

import sys
from configparser import ConfigParser
from confluent_kafka import Consumer

if __name__ == '__main__':
    # Parse config from file `config.ini`
    config_parser = ConfigParser()
    config_parser.read('config.ini')
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    consumer.subscribe(['{{ cc_topic }}'])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            record = consumer.poll(1.0)
            if record is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print('Waiting...')
            elif record.error():
                print('ERROR: %s' % record.error())
            else:
                # Extract the (optional) key and value, and print.
                print((f'topic: {record.topic()} '
                       f'key: {record.key().decode("utf-8")} '
                       f'value: {record.value().decode("utf-8")}'))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
