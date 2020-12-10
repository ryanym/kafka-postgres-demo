import unittest
import json
import string
from producer import send_message, setup_producer
from consumer import receive_message, store_messages, setup_consumer
from utils import *
from kafka import KafkaProducer, KafkaConsumer


class TestConsumerPostgres(unittest.TestCase):
    def setUp(self):
        self.producer_config = parse_config('producer_config.yaml')
        self.consumer_config = parse_config('consumer_config.yaml')

        self.producer = setup_producer(self.producer_config)
        self.consumer = setup_consumer(self.consumer_config)


        self.conn = create_connection(self.consumer_config['postgres']['host'],
                                      self.consumer_config['postgres']['port'],
                                      self.consumer_config['postgres']['database'],
                                      self.consumer_config['postgres']['user'],
                                      self.consumer_config['postgres']['password'])
        self.table_name = self.consumer_config['postgres']['table']
        self.column_name = self.consumer_config['postgres']['column']
        drop_table(self.conn, self.table_name)
        create_table_if_not_exists(self.conn,
                                   self.table_name,
                                   self.column_name)

    def test_consumer_store_to_postgres(self):
        for msg in string.ascii_lowercase:
            ret = send_message(self.producer,
                               self.producer_config['kafka']['topic'],
                               self.producer_config['kafka']['message_key'],
                               msg)
        received_message = receive_message(self.consumer, self.consumer_config['kafka']['message_key'])

        store_messages(received_message,
                       self.conn,
                       self.table_name,
                       self.column_name)

        cur = self.conn.cursor()
        cur.execute(f'select * from {self.table_name}')
        stored_message_str = ''

        for record in cur:
            msg = record[0]
            stored_message_str += msg

        self.assertEqual(string.ascii_lowercase, stored_message_str)

    def tearDown(self):
        # consume all messages left on the queue
        received_message = receive_message(self.consumer, self.consumer_config['kafka']['message_key'])