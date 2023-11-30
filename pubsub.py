import argparse
import json
import logging
from queue import Queue, PriorityQueue
from threading import Lock

from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from kafka.admin import NewTopic

logger = logging.getLogger(__name__)

BROKER_PORT = 8010
class PubSubBase:
    def __init__(self, use_kafka, max_queue_size, max_id_per_topic):
        self.topics = {}
        self.count = {}
        self.topics_lock = Lock()
        self.count_lock = Lock()

        self.use_kafka = use_kafka
        self.kafka_producer_client = None
        self.max_queue_size = max_queue_size
        self.max_id_per_topic = max_id_per_topic

    def subscribe_(self, listener, topic, is_priority_queue):
        if not topic:
            raise ValueError('topic: cannot be None')

        with self.topics_lock:
            if topic not in self.topics:
                self.topics[topic] = {}

        if self.use_kafka:
            try:
                admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
                topic_metadata = admin.list_topics()
                if topic not in topic_metadata:
                    channel_topic = NewTopic(name=topic,
                                             num_partitions=1,
                                             replication_factor=1)
                    admin.create_topics([channel_topic])
                message_queue = topic
                admin.close()
                self.kafka_producer_client = KafkaProducer(
                    bootstrap_servers='localhost:9092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=0
                )
            except Exception as e:
                logger.error("Kafka init error", e)
                raise
        else:
            message_queue = PriorityQueue() if is_priority_queue else Queue()
        self.topics[topic][listener] = message_queue

        return message_queue

    def unsubscribe(self, listener, topic):
        if not topic:
            raise ValueError('topic: cannot be None')
        if topic in self.topics:
            if listener in self.topics[topic]:
                del self.topics[topic][listener]

    def publish_(self, topic, message, is_priority_queue, priority):
        if not message:
            raise ValueError('message: cannot be None')
        if not topic:
            raise ValueError('topic: cannot be None')
        if not priority >= 0:
            raise ValueError('priority: must be > 0')

        with self.topics_lock:
            if topic not in self.topics:
                self.topics[topic] = {}

        with self.count_lock:
            self.count[topic] = (self.count.get(topic, 0) + 1) % self.max_id_per_topic

        _id = self.count[topic]

        for listener in self.topics[topic]:
            topic_queue = self.topics[topic][listener]
            if self.use_kafka:
                try:
                    self.kafka_producer_client.send(
                        topic_queue,
                        value={'topic': topic, 'data': message, 'id': _id}
                    )
                except Exception as e:
                    self.kafka_producer_client.close()
                    logger.error("Kafka producer error: ", e)
                    raise
            else:
                if topic_queue.qsize() >= self.max_queue_size:
                    logger.warning(
                        f"Queue overflow for topic {topic}, "
                        f"> {self.max_queue_size} "
                        "(self.max_id_per_topic parameter)")
                else:
                    if is_priority_queue:
                        topic_queue.put((priority, {'data': message, 'id': _id}), block=False)
                    else:
                        topic_queue.put({'topic': topic, 'data': message, 'id': _id}, block=False)

    def get_message(self, listener, topic):
        message_queue = self.topics.get(topic, {}).get(listener, None)
        if message_queue is None:
            return None
        try:
            if self.use_kafka:
                consumer = KafkaConsumer(
                    message_queue,
                    bootstrap_servers='localhost:9092',
                    auto_offset_reset='earliest',
                    group_id=listener,
                    enable_auto_commit=False,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                )
                topic_partition = TopicPartition(topic, 0)
                messages = consumer.poll(timeout_ms=100)
                if not messages or messages == {}:
                    return None
                else:
                    print(f"Total Messages in Kafka Queue for {topic_partition}: {len(messages[topic_partition])}")
                    data = messages[topic_partition][-1].value
                    consumer.commit(offsets={topic_partition: OffsetAndMetadata(messages[topic_partition][-1].offset + 1, None)})
                    return data
            else:
                return message_queue.get_nowait()
        except Exception as e:
            # logger.error("Consumer error", e)
            return None


class PubSub(PubSubBase):
    def __init__(self, use_kafka):
        super().__init__(use_kafka, 100, 2 ** 30)

    def subscribe(self, listener, topic):
        return self.subscribe_(listener, topic, False)

    def publish(self, topic, message):
        self.publish_(topic, message, False, priority=100)

    def unsubscribe(self, listener, topic):
        super().unsubscribe(listener, topic)

    def listen(self, listener, topic):
        return self.get_message(listener, topic)


def main():
    parser = argparse.ArgumentParser(description='Broker program for emitting crypto prices')
    parser.add_argument('--useKafka', action='store_true', default=False, help='Flag to turn on Kafka message broker')

    args = parser.parse_args()
    # Access the flags
    use_kafka = args.useKafka

    # Create an instance of the PubSub class
    broker = PubSub(use_kafka)

    # Define the functions to expose over JSON-RPC
    def rpc_publish(topic, message):
        broker.publish(topic, message)

    def rpc_subscribe(listener, topic):
        return broker.subscribe(listener, topic)

    def rpc_listen(listener, topic):
        return broker.listen(listener, topic)

    def rpc_unsubscribe(listener, topic):
        broker.unsubscribe(listener, topic)

    # Set up the JSON-RPC server
    server = SimpleJSONRPCServer(('localhost', BROKER_PORT))
    server.register_function(rpc_subscribe, 'subscribe')
    server.register_function(rpc_unsubscribe, 'unsubscribe')
    server.register_function(rpc_listen, 'listen')
    server.register_function(rpc_publish, 'publish')
    server.allow_reuse_address = True

    # Start the server
    print(f"Broker started on port {BROKER_PORT}...")
    server.serve_forever()


if __name__ == '__main__':
    main()
