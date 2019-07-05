import os.path
import random
import time
import sys

from confluent_kafka.avro import AvroProducer

WARNINGS = ['w1', 'w2', 'w3', 'w4']
ERRORS = ['e1', 'e2', 'e3', 'e4']
schema_dir = os.path.dirname(os.path.dirname(__file__)) + "/schemas/"

if len(sys.argv) >= 2:
    device_id = sys.argv[1]
else:
    device_id = 'N0001'

avro_producer = AvroProducer({
        'bootstrap.servers': ':9092',
        'schema.registry.url': 'http://...:8081'
    })

def load_schema_file(fname):
    with open(fname) as f:
        return f.read()


KEY_SCHEMA = load_schema_file(os.path.join(schema_dir, 'key-schema.avsc'))
ERROR_SCHEMA = load_schema_file(os.path.join(schema_dir, 'error-schema.avsc'))
WARNING_SCHEMA = load_schema_file(os.path.join(schema_dir, 'warning-schema.avsc'))


def create_warning_item():
    return {
        'DeviceID': device_id,
        'timestamp': int(round(time.time() * 1000)),
        'warning': random.choice(WARNINGS),
    }


def create_error_item():
    return {
        'DeviceID': device_id,
        'timestamp': int(round(time.time() * 1000)),
        'error': random.choice(ERRORS),
    }


def main():

    while True:
        r = random.choice(['e', 'w'])

        if r == 'e':
            value = create_error_item()
            key = {'DeviceID': 'N0001'}

            print(key)
            print(value)

        else:
            value = create_warning_item()
            key = {'DeviceID': 'N0001'}

            print(key)
            print(value)


# def main():
#
#     while True:
#         r = random.choice(['e', 'w'])
#
#         if r == 'e':
#             value = create_error_item()
#             key = {'DeviceID': 'N0001'}
#
#             avro_producer.produce(topic='errors', value=value, value_schema=ERROR_SCHEMA, key=key,
#                                   key_schema=KEY_SCHEMA)
#             avro_producer.flush()
#         else:
#             value = create_warning_item()
#             key = {'DeviceID': 'N0001'}
#
#             avro_producer.produce(topic='warnings', value=value, value_schema=WARNING_SCHEMA, key=key,
#                                   key_schema=KEY_SCHEMA)
#             avro_producer.flush()


if __name__ == "__main__":
    main()
