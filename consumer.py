import json
import argparse
import os
from utils import *
from kafka import KafkaConsumer


def receive_message(consumer, kafka_message_key):
    """
    Receive(Consume) messages from topic
    :param consumer(KafkaConsumer): configured Kafka consumer to receive the messages
    :param kafka_message_key(str): key of serialized data
    :return: list of messages(str)
    """
    received_message = []
    raw_data = consumer.poll(10000)
    for partition, records in raw_data.items():
        for record in records:
            print(record)
            msg = record.value[kafka_message_key]
            print(f'received message {msg}')
            received_message.append(msg)
    consumer.commit()
    print('finished receiving messages')
    return received_message


def store_messages(received_message, conn, table_name, column_name):
    """
    Store messages to postgres db
    :param received_message(list(str)): a list of messages to store
    :param conn(connection): postgres db connection
    :param table_name(str): table name to store messages in
    :param column_name(str): column name to store messages in
    :return: 0 if successful and -1 otherwise
    """
    if len(received_message) == 0:
        print("No received message found")
        return -1

    for message in received_message:
        insert_message(conn, table_name, column_name, message)
        print(f'stored message {message} to table {table_name}')
    return 0


def setup_consumer(config):
    """
    set up kafka consumer with config
    :param config(dict): a python dict of consumer configuration options. See example consumer configuration.
    :return: KafkaConsumer
    """
    pwd = os.path.dirname(os.path.realpath(__file__))

    consumer = KafkaConsumer(config['kafka']['topic'],
                             bootstrap_servers=config['kafka']['brokers'],
                             auto_offset_reset=config['kafka']['offset_reset'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                             group_id=config['kafka']['group_id'],
                             ssl_cafile=pwd + '/' + config['kafka']['ssl_cafile'],
                             ssl_certfile=pwd + '/' + config['kafka']['ssl_certfile'],
                             ssl_keyfile=pwd + '/' + config['kafka']['ssl_keyfile'],
                             security_protocol='SSL',
                             )
    return consumer


def main():
    """
    Parse command line arguments and receive messages from kafka brokers and store messages to postgres db
    :return: return 0 if successful, negative numbers otherwise
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', required=True,
                        default='producer_config.yaml',
                        help='kafka consumer config file')
    args = parser.parse_args()

    config = parse_config(args.config)

    # setup postgres database connection and related variables
    conn = create_connection(config['postgres']['host'],
                             config['postgres']['port'],
                             config['postgres']['database'],
                             config['postgres']['user'],
                             config['postgres']['password'])
    if conn is None:
        raise Exception("Cannot create database connection")

    table_name = config['postgres']['table']
    column_name = config['postgres']['column']
    create_table_if_not_exists(conn, table_name, column_name)

    # setup consumer and related variables
    consumer = setup_consumer(config)
    kafka_message_key = config['kafka']['message_key']

    # consume and store messages
    received_message = receive_message(consumer, kafka_message_key)
    ret = store_messages(received_message, conn, table_name, column_name)

    conn.close()
    consumer.close()

    return ret


if __name__ == '__main__':
    main()
