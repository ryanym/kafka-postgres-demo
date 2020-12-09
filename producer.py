import time
import json
import string
from kafka import KafkaProducer
KAFKA_BROKER = '127.0.0.1:9092'
KAFKA_TOPIC = 'testtopic'


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    for i in string.ascii_letters:
        data = {'message': i}
        print(f'Sending char: {data}')
        producer.send(KAFKA_TOPIC, data)
        time.sleep(10)


if __name__ == '__main__':
    main()
