#!/usr/bin/env python

import random
import string
import sys
from configparser import ConfigParser
import time
from confluent_kafka import Producer

if __name__ == '__main__':
    # Parse config from file `config.ini`
    config_parser = ConfigParser()
    config_parser.read('config.ini')
    config = dict(config_parser['default'])

    # creates a new producer instance
    producer = Producer(config)

    # produces message until terminated
    try:
        while True:
            # produces a sample message
            key = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
            value = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            producer.produce("{{ cc_topic }}", key=key, value=value)
            print(f"Produced message to topic {{ cc_topic }}: key = {key:12} value = {value:12}")

            # send any outstanding or buffered messages to the Kafka broker
            producer.flush()

            # wait 1sec before next message
            time.sleep(1)
    except KeyboardInterrupt:
        pass