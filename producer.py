import json
import argparse
import os
from utils import *
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError


def send_message(producer, topic, kafka_message_key, message):
    """
    send message to kafka topic in key-value format
    :param producer(KafkaProducer): kafka producer to publish the message
    :param topic(str): kafka topic to publish the message to
    :param kafka_message_key(str): key of the message
    :param message(str): message to send
    :return: FutureRecordMetadata this is used to validate if message has been sent
    """
    data = {kafka_message_key: message}
    record = None
    try:
        record = producer.send(topic, data)
        print(f'Sent message: {data} to topic: {topic}, record: {record}')
    except KafkaTimeoutError as e:
        print(e)
    return record


def setup_producer(config):
    """
    set up kafka producer with config
    :param config(dict): a python dict of producer configuration options. See example producer configuration.
    :return: KafkaProducer
    """
    pwd = os.path.dirname(os.path.realpath(__file__))
    producer = KafkaProducer(bootstrap_servers=config['kafka']['brokers'],
                             security_protocol='SSL',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                             ssl_cafile=pwd + '/' + config['kafka']['ssl_cafile'],
                             ssl_certfile=pwd + '/' + config['kafka']['ssl_certfile'],
                             ssl_keyfile=pwd + '/' + config['kafka']['ssl_keyfile'],
                             )
    return producer


def main():
    """
    Parse command line arguments and publish the message from cli with kafka producer
    :return: return 0 if successful, negative numbers otherwise
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', required=True,
                        default='producer_config.yaml',
                        help='kafka producer config file')
    parser.add_argument('-m', '--message', action='store', required=True,
                        default=None,
                        help='message to send to kafka topic')
    args = parser.parse_args()

    config = parse_config(args.config)

    # setup producer and related info
    producer = setup_producer(config)
    topic = config['kafka']['topic']
    kafka_message_key = config['kafka']['message_key']

    # publish messages
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
