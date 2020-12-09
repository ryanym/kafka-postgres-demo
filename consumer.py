import time
import json
from kafka import KafkaConsumer
from db_utils import *


KAFKA_BROKER = '127.0.0.1:9092'
KAFKA_TOPIC = 'testtopic'
POSTGRES_HOST = '127.0.0.1'
POSTGRES_PORT = 5432
DATABASE_NAME = 'testdb'
DATABASE_USER = 'testuser'
DATABASE_PASSWD = 'testpass'
TABLE_NAME = 'testtable'
COLUMN_NAME = 'message'


def main():
    conn = create_connection(POSTGRES_HOST,
                             POSTGRES_PORT,
                             DATABASE_NAME,
                             DATABASE_USER,
                             DATABASE_PASSWD)
    if conn is None:
        raise Exception("Cannot create database connection")

    create_table_if_not_exists(conn, TABLE_NAME, COLUMN_NAME)

    consumer = KafkaConsumer(KAFKA_TOPIC,
                             bootstrap_servers=KAFKA_BROKER,
                             auto_offset_reset='earliest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for record in consumer:
        msg = record.value['message']
        print(f'received message {msg}')
        insert_message(conn, TABLE_NAME, COLUMN_NAME, msg)

    conn.close()


if __name__ == '__main__':
    main()