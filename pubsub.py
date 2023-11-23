import warnings
from threading import Lock, Thread
from queue import Queue, PriorityQueue, Empty
from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer

class PubSubBase():
    def __init__(self, max_queue_in_a_channel=100, max_id_4_a_channel=2**31):
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

        message_queue = PriorityQueue() if is_priority_queue else Queue()
        self.channels[channel][listener] = message_queue

        return message_queue

    def unsubscribe(self, listener, channel, message_queue):
        if not channel:
            raise ValueError('channel : None value not allowed')
        if not message_queue:
            raise ValueError('message_queue : None value not allowed')
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
            if channel_queue.qsize() >= self.max_queue_in_a_channel:
                warnings.warn(
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
            return message_queue.get_nowait()
        except Empty:
            return None

class PubSub(PubSubBase):
    def subscribe(self, listener, channel):
        return self.subscribe_(listener, channel, False)

    def publish(self, channel, message):
        self.publish_(channel, message, False, priority=100)

    def getMessageQueue(self, listener, channel_name):
        return self.getMessageQueue_(listener, channel_name)

    def unsubscribe(self, listener, channel_name):
        message_queue = self.getMessageQueue(listener, channel_name)
        if message_queue:
            super().unsubscribe(listener, channel_name, message_queue)

    def listen(self, listener, channel_name):
        return self.get_message(listener, channel_name)

def main():
    # Create an instance of the PubSub class
    communicator = PubSub()

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
    server = SimpleJSONRPCServer(('localhost', 1006))
    server.register_function(rpc_publish, 'publish')
    server.register_function(rpc_subscribe, 'subscribe')
    server.register_function(rpc_listen, 'listen')
    server.register_function(rpc_unsubscribe, 'unsubscribe')

    # Start the server
    print("Broker started")
    server.serve_forever()

if __name__ == '__main__':
    main()
