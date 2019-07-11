import json
import os.path
import random
import time
import sys

import avro
from confluent_kafka.avro import AvroProducer
import time

from numpy.core import long

WARNINGS = ['w1', 'w2', 'w3', 'w4']
ERRORS = ['e1', 'e2', 'e3', 'e4']
schema_dir = os.path.dirname(os.path.dirname(__file__)) + "/schemas/"

if len(sys.argv) >= 2:
    device_id = sys.argv[1]
    base_uri = sys.argv[2]
else:
    device_id = 'N0001'
    base_uri = 'ec2-63-34-70-213.eu-west-1.compute.amazonaws.com'


avro_producer = AvroProducer({
        'bootstrap.servers': base_uri + ':9092',
        'schema.registry.url': 'http://' + base_uri + ':8081'
    })

def load_schema_file(fname):
    with open(fname) as f:
        return f.read()


KEY_SCHEMA = load_schema_file(os.path.join(schema_dir, 'key-schema.avsc'))
ERROR_SCHEMA = load_schema_file(os.path.join(schema_dir, 'error-schema.avsc'))
WARNING_SCHEMA = load_schema_file(os.path.join(schema_dir, 'warning-schema.avsc'))


def create_warning_item():
    return {
        'deviceID': device_id,
        'timestamp': str(int(round(time.time() * 1000))),
        'warning': random.choice(WARNINGS)
    }


def create_error_item():
    return {
        'deviceID': device_id,
        'timestamp': str(int(round(time.time() * 1000))),
        'error': random.choice(ERRORS)
    }


def main():

    while True:
        r = random.choice(['e', 'w'])

        if r == 'e':
            value = create_error_item()
            key = {'deviceID': device_id}

            avro_producer.produce(topic='errors2',
                                  value=value,
                                  value_schema=avro.schema.Parse(ERROR_SCHEMA),
                                  key=key,
                                  key_schema=avro.schema.Parse(KEY_SCHEMA)
                                  )
            avro_producer.flush()
        else:
            value = create_warning_item()
            key = {'deviceID': device_id}

            avro_producer.produce(topic='warnings2',
                                  value=value,
                                  value_schema=avro.schema.Parse(WARNING_SCHEMA),
                                  key=key,
                                  key_schema=avro.schema.Parse(KEY_SCHEMA)
                                  )
            avro_producer.flush()

        print(value)
        time.sleep(2)


if __name__ == "__main__":
    main()
