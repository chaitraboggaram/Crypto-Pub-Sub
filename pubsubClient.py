from http.server import BaseHTTPRequestHandler, HTTPServer
from jsonrpclib import Server
import socket
import threading
import json
import time
import logging

logger = logging.getLogger(__name__)


HOST = "localhost"
PORT = 8000  # default
data = {}
listener_track = {}

class Listener(threading.Thread):
    def __init__(self, channel_name, thread_name):
        threading.Thread.__init__(self)
        self.channel_name = channel_name
        self.broker = Server('http://localhost:5000')
        self.thread_name = thread_name
        self.message_queue = self.broker.subscribe(thread_name, channel_name)
        self.daemon = True  # Ensure thread exits when main program does
        self._stop_event = threading.Event()

    def run(self):
        print(f"{self.thread_name} Run start, listening to messages on channel {self.channel_name}")
        global data
        while not self._stop_event.is_set():
            message = self.broker.listen(self.thread_name, self.channel_name)
            if message:
                print(f"Received message: {message}")
                data = message
                # Process the message here
            else:
                # No message in queue, sleep for a short time to avoid busy waiting
                time.sleep(0.5)
                self._stop_event.wait(timeout=0.5)

    def stop(self):
        self._stop_event.set()

    def unsubscribe(self):
        self.broker.unsubscribe(self.thread_name, self.channel_name)


def send_success_response(client):
    client.sendall(str.encode("HTTP/1.1 200 OK\n", 'iso-8859-1'))
    client.sendall(str.encode('Content-Type: application/json\n', 'iso-8859-1'))
    client.sendall(str.encode('Access-Control-Allow-Origin: *\n', 'iso-8859-1'))
    client.sendall(b'0\r\n\r\n')


def send_data(client, listener_name):
    global data
    json_string = json.dumps(data.get(listener_name, []))
    client.sendall(str.encode("HTTP/1.1 200 OK\n", 'iso-8859-1'))
    client.sendall(str.encode('Content-Type: application/json\n', 'iso-8859-1'))
    client.sendall(str.encode('Access-Control-Allow-Origin: *\n', 'iso-8859-1'))
    client.sendall(str.encode('\r\n'))
    client.sendall(json_string.encode())
    client.sendall(b'0\r\n\r\n')


class ThreadedServer(object):
    def __init__(self):
        print("Client started")

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((HOST, PORT))
            sock.listen()
            while True:
                conn, addr = sock.accept()
                t = threading.Thread(target=self.handle_client, args=(conn, addr))
                t.start()

    def handle_client(self, client, address):
        size = 1024
        with client:
            while True:
                try:
                    global listener_track
                    request = client.recv(size).decode()
                    headers = request.split('\r\n')
                    REST = headers[0].split()
                    if "/subscribe" in REST[1]:
                        query = REST[1].split('?')[1]
                        params = dict(x.split('=') for x in query.split('&'))
                        listener_name = params['listener']
                        channel_name = params['channel']
                        listener = Listener(channel_name, listener_name)
                        if listener_name not in listener_track:
                            listener_track[listener_name] = {}
                        listener_track[listener_name][channel_name] = listener
                        listener.start()
                        send_success_response(client)
                    elif "/getData" in REST[1]:
                        query = REST[1].split('?')[1]
                        params = dict(x.split('=') for x in query.split('&'))
                        listener_name = params['listener']
                        send_data(client, listener_name)
                    elif "/unsubscribe" in REST[1]:
                        query = REST[1].split('?')[1]
                        params = dict(x.split('=') for x in query.split('&'))
                        listener_name = params['listener']
                        channel_name = params['channel']
                        if listener_name in listener_track and channel_name in listener_track[listener_name]:
                            listener_track[listener_name][channel_name].stop()
                            listener_track[listener_name][channel_name].join()
                            listener_track[listener_name][channel_name].unsubscribe()
                        send_success_response(client)
                except Exception as e:
                    logger.error("Caught an Exception", e)
                    break
            client.close()


def main():
    ThreadedServer().listen()

if __name__ == '__main__':
    main()
