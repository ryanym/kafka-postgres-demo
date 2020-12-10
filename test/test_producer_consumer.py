import unittest
import json
import string
from producer import send_message, setup_producer
from consumer import receive_message, setup_consumer
from utils import parse_config
from kafka import KafkaProducer, KafkaConsumer


class TestProducerConsumer(unittest.TestCase):
    def setUp(self):
        self.producer_config = parse_config('../producer_config.yaml')
        self.consumer_config = parse_config('../consumer_config.yaml')

        self.producer = setup_producer(self.producer_config)
        self.consumer = setup_consumer(self.consumer_config)

    def test_producer_send_message(self):

        for msg in string.ascii_lowercase:
            ret = send_message(self.producer,
                               self.producer_config['kafka']['topic'],
                               self.producer_config['kafka']['message_key'],
                               msg)

            self.assertIsNotNone(ret)

    def test_consumer_receive_message(self):
        # in case topic is empty
        self.test_producer_send_message()
        received_message = receive_message(self.consumer, self.consumer_config['kafka']['message_key'])
        self.assertGreater(len(received_message), 0, 'consumer did not receive any message')

    def test_producer_consumer(self):
        for msg in string.ascii_lowercase:
            ret = send_message(self.producer,
                               self.producer_config['kafka']['topic'],
                               self.producer_config['kafka']['message_key'],
                               msg)
        received_message = receive_message(self.consumer, self.consumer_config['kafka']['message_key'])

        received_message_str = ''.join(received_message)
        self.assertEqual(string.ascii_lowercase, received_message_str)

    def tearDown(self):
        # consume all messages left on the queue
        received_message = receive_message(self.consumer, self.consumer_config['kafka']['message_key'])
