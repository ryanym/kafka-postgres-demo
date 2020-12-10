# kafka-postgres-demo
## Overview
Kafka Postgres demo project. This project contains two cli tools:   
- a Kafka producer which takes an input messages and publishes it to a topic
- a Kafka consumer which subscribes to the topic and stores received messages to a postgres database

## Requirements
- [ ] Kafka broker instance
- [ ] Postgres instance
- [ ] `requirements.txt`

## Example
Configure Kafka producer/consumer and Postgres connection info in `producer_config.yaml` and `consumer_config.yaml`

Send messages to broker
```
python3 producer.py -c producer_config.yaml -m 'test foobar'
Sent message: {'message': 'test foobar'} to topic: testtopic, record: <kafka.producer.future.FutureRecordMetadata object at 0x7f4030eccd60>
```
Receive messages from broker
```
python3 consumer.py -c consumer_config.yaml
ConsumerRecord(topic='testtopic', partition=0, offset=754, timestamp=1607566855796, timestamp_type=0, key=None, value={'message': 'test foobar'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=26, serialized_header_size=-1)
received message test foobar
finished receiving messages
stored message test foobar to table testtable
```

## Usage
producer
```
python3 consumer.py -c consumer_config.yaml
ConsumerRecord(topic='testtopic', partition=0, offset=754, timestamp=1607566855796, timestamp_type=0, key=None, value={'message': 'test foobar'}, headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=26, serialized_header_size=-1)
received message test foobar
finished receiving messages
stored message test foobar to table testtable
```
consumer
```
python3 consumer.py -h                     
usage: consumer.py [-h] -c CONFIG

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        kafka consumer config file
```
## Tests

producer to consumer
This test suit tests the following scenarios:
1. test producer send to topic
2. test consumer receive from topic (implicitly test 1)
3. test producer send to topic and consumer receive the same messages in the same order (implicitly test 1 and 2)
```
python3 -m unittest -v test.test_producer_consumer
```
producer to consumer to postgres
This test suit tests the following scenarios:
1. test producer send to topic and consumer receive the same messages and store messages in postgres database.  
the messages in test table needs to be the same as the ones that the producer sent
```
python3 -m unittest -v test.test_consumer_postgres
```
## Reference
- https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
- https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
