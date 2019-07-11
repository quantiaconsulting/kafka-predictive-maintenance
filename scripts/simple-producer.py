import random
import sys
import time
import json

from confluent_kafka import Producer

WARNINGS = ['w1', 'w2', 'w3', 'w4']
ERRORS = ['e1', 'e2', 'e3', 'e4']

if len(sys.argv) >= 2:
    device_id = sys.argv[1]
    base_uri = sys.argv[2]
else:
    device_id = 'N0001'
    base_uri = 'ec2-63-34-70-213.eu-west-1.compute.amazonaws.com'

p = Producer({'bootstrap.servers': base_uri + ':9092'})


def create_warning_item():

    data = {'deviceID': device_id,
            'timestamp': str(int(round(time.time() * 1000))),
            'warning': random.choice(WARNINGS)}
    return json.dumps(data)

def create_error_item():

    data = {'deviceID': device_id,
            'timestamp': str(int(round(time.time() * 1000))),
            'error': random.choice(ERRORS)}
    return json.dumps(data)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():

    while True:
        r = random.choice(['e', 'w'])

        if r == 'e':
            value = create_error_item()

            p.produce('errors-simple', str(value).encode('utf-8'), callback=delivery_report)
            p.flush()
        else:
            value = create_warning_item()

            p.produce('warnings-simple', str(value).encode('utf-8'), callback=delivery_report)
            p.flush()

        print(value)
        time.sleep(2)


if __name__ == "__main__":
    main()
