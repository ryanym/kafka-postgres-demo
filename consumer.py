import json
import argparse
from utils import *
from kafka import KafkaConsumer

def receive_message(consumer, kafka_message_key):

    received_message = []
    raw_data = consumer.poll(1000)
    for partition, records in raw_data.items():
        for record in records:
            print(record)
            msg = record.value[kafka_message_key]
            print(f'received message {msg}')
            received_message.append(msg)
    print('finished receiving messages')
    return received_message

def store_messages(received_message, conn, table_name, column_name):
    if len(received_message) == 0:
        print("No received message found")
        return -1

    for message in received_message:
        insert_message(conn, table_name, column_name, message)
        print(f'stored message {message} to table {table_name}')
    return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', action='store', required=True,
                        default='producer_config.yaml',
                        help='kafka consumer config file')
    args = parser.parse_args()

    config = parse_config(args.config)

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

    consumer = KafkaConsumer(config['kafka']['topic'],
                             bootstrap_servers=config['kafka']['brokers'],
                             auto_offset_reset=config['kafka']['offset_reset'],
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                             group_id=config['kafka']['group_id'],
                             )
    kafka_message_key = config['kafka']['message_key']

    received_message = receive_message(consumer, kafka_message_key)
    ret = store_messages(received_message, conn, table_name, column_name)

    conn.close()
    consumer.close()


if __name__ == '__main__':
    main()