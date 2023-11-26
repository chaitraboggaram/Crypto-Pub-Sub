import logging
from threading import Lock, Thread
from queue import Queue, PriorityQueue, Empty
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
import argparse
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from kafka.admin import NewTopic
import json

logger = logging.getLogger(__name__)


class PubSubBase:
    def __init__(self, use_kafka, max_queue_in_a_channel=100, max_id_4_a_channel=2 ** 31):
        self.use_kafka = use_kafka
        self.kafka_producer_client = None
        self.max_queue_in_a_channel = max_queue_in_a_channel
        self.max_id_4_a_channel = max_id_4_a_channel

        self.channels = {}
        self.count = {}

        self.channels_lock = Lock()
        self.count_lock = Lock()

    def subscribe_(self, listener, channel, is_priority_queue):
        if not channel:
            raise ValueError('channel : None value not allowed')

        with self.channels_lock:
            if channel not in self.channels:
                self.channels[channel] = {}

        if self.use_kafka:
            try:
                admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
                topic_metadata = admin.list_topics()
                if channel not in topic_metadata:
                    channel_topic = NewTopic(name=channel,
                                             num_partitions=1,
                                             replication_factor=1)
                    admin.create_topics([channel_topic])
                message_queue = channel
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
        self.channels[channel][listener] = message_queue

        return message_queue

    def unsubscribe(self, listener, channel):
        if not channel:
            raise ValueError('channel : None value not allowed')
        if channel in self.channels:
            if listener in self.channels[channel]:
                del self.channels[channel][listener]

    def publish_(self, channel, message, is_priority_queue, priority):
        if priority < 0:
            raise ValueError('priority must be > 0')
        if not channel:
            raise ValueError('channel : None value not allowed')
        if not message:
            raise ValueError('message : None value not allowed')

        with self.channels_lock:
            if channel not in self.channels:
                self.channels[channel] = {}

        with self.count_lock:
            self.count[channel] = (self.count.get(channel, 0) + 1) % self.max_id_4_a_channel

        _id = self.count[channel]

        for listener in self.channels[channel]:
            channel_queue = self.channels[channel][listener]
            if self.use_kafka:
                try:
                    self.kafka_producer_client.send(
                        channel_queue,
                        value={'channel': channel, 'data': message, 'id': _id}
                    )
                except Exception as e:
                    self.kafka_producer_client.close()
                    logger.error("Kafka producer error: ", e)
                    raise
            else:
                if channel_queue.qsize() >= self.max_queue_in_a_channel:
                    logger.warning(
                        f"Queue overflow for channel {channel}, "
                        f"> {self.max_queue_in_a_channel} "
                        "(self.max_queue_in_a_channel parameter)")
                else:
                    if is_priority_queue:
                        channel_queue.put((priority, {'data': message, 'id': _id}), block=False)
                    else:
                        channel_queue.put({'channel': channel, 'data': message, 'id': _id}, block=False)

    def get_message(self, listener, channel):
        message_queue = self.channels.get(channel, {}).get(listener, None)
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
                topic_partition = TopicPartition(channel, 0)
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
            logger.error("Consumer error", e)
            return None


class PubSub(PubSubBase):
    def __init__(self, use_kafka):
        super().__init__(use_kafka)

    def subscribe(self, listener, channel):
        return self.subscribe_(listener, channel, False)

    def publish(self, channel, message):
        self.publish_(channel, message, False, priority=100)

    def unsubscribe(self, listener, channel_name):
        super().unsubscribe(listener, channel_name)

    def listen(self, listener, channel_name):
        return self.get_message(listener, channel_name)


def main():
    parser = argparse.ArgumentParser(description='Broker program for emitting crypto prices')
    parser.add_argument('--useKafka', action='store_true', default=False, help='Flag to turn on Kafka message broker')

    args = parser.parse_args()

    # Access the flags
    use_kafka = args.useKafka

    # Create an instance of the PubSub class
    communicator = PubSub(use_kafka)

    # Define the functions to expose over JSON-RPC
    def rpc_publish(channel_name, message):
        communicator.publish(channel_name, message)

    def rpc_subscribe(listener, channel_name):
        return communicator.subscribe(listener, channel_name)

    def rpc_listen(listener, channel_name):
        return communicator.listen(listener, channel_name)

    def rpc_unsubscribe(listener, channel_name):
        communicator.unsubscribe(listener, channel_name)

    # Set up the JSON-RPC server
    server = SimpleJSONRPCServer(('localhost', 5000))
    server.register_function(rpc_publish, 'publish')
    server.register_function(rpc_subscribe, 'subscribe')
    server.register_function(rpc_listen, 'listen')
    server.register_function(rpc_unsubscribe, 'unsubscribe')

    # Start the server
    print("Broker started")
    server.serve_forever()


if __name__ == '__main__':
    main()
