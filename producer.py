import json
import argparse
from utils import *
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError


def send_message(producer, topic, kafka_message_key, message):
    data = {kafka_message_key: message}
    record = None
    try:
        record = producer.send(topic, data)
        print(f'Sent message: {data} to topic: {topic}')
    except KafkaTimeoutError as e:
        print(e)
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', required=True,
                        default='producer_config.yaml',
                        help='kafka producer config file')
    parser.add_argument('-m', '--message', action='store', required=True,
                        default=None,
                        help='message to send to kafka topic')
    args = parser.parse_args()

    config = parse_config(args.config)
    producer = KafkaProducer(bootstrap_servers=config['kafka']['brokers'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    topic = config['kafka']['topic']
    kafka_message_key = config['kafka']['message_key']

    message = args.message

    if message:
        ret = send_message(producer, topic, kafka_message_key, message)
        if ret is None:
            print('Failed to send message: producer timed out')
            return -1
    else:
        print('Message is empty, program exited')
        return -2

    producer.close()
    return 0


if __name__ == '__main__':
    main()
