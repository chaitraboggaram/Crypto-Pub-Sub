import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from jsonrpclib import Server

logger = logging.getLogger(__name__)

PORT = 8000
data = {}
listener_track = {}


class Listener(threading.Thread):
    def __init__(self, topic_name, listener_name):
        threading.Thread.__init__(self)
        self.topic_name = topic_name
        self.broker = Server('http://localhost:8010')
        self.thread_name = listener_name
        self.message_queue = self.broker.subscribe(listener_name, topic_name)
        self._stop_event = threading.Event()

    def run(self):
        print(f"{self.thread_name} Run start, listening to messages on topic {self.topic_name}")
        global data
        while not self._stop_event.is_set():
            message = self.broker.listen(self.thread_name, self.topic_name)
            if message:
                if self.thread_name not in data:
                    data[self.thread_name] = {}
                data[self.thread_name][self.topic_name] = message
            else:
                # No message in queue, sleep for a short time to avoid busy waiting
                time.sleep(0.5)
                self._stop_event.wait(timeout=0.5)

    def stop(self):
        self._stop_event.set()

    def unsubscribe(self):
        self.broker.unsubscribe(self.thread_name, self.topic_name)


class Handler(BaseHTTPRequestHandler):
    global data
    global listener_track

    def do_GET(self):
        try:
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            query_params = parse_qs(parsed_url.query)

            if path == '/subscribe':
                listener_name = query_params['listener'][0]
                topic_name = query_params['topic'][0]
                listener = Listener(topic_name, listener_name)
                if listener_name not in listener_track:
                    listener_track[listener_name] = {}
                listener_track[listener_name][topic_name] = listener
                listener.start()
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

            elif path == "/getData":
                listener_name = query_params['listener'][0]
                topic_name = query_params['topic'][0]
                if data.get(listener_name):
                    response_data = data.get(listener_name).get(topic_name, [])
                    json_string = json.dumps([response_data]) if response_data != [] else json.dumps(response_data)
                else:
                    json_string = json.dumps([])
                print(f"Response JSON: {json_string}")
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

                self.wfile.write(json_string.encode('utf-8'))

            elif path == "/unsubscribe":
                listener_name = query_params['listener'][0]
                topic_name = query_params['topic'][0]
                if listener_name in listener_track and topic_name in listener_track[listener_name]:
                    listener_track[listener_name][topic_name].stop()
                    listener_track[listener_name][topic_name].join()
                    listener_track[listener_name][topic_name].unsubscribe()

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()

        except Exception as e:
            logger.error("Caught an Exception: ", e)
            pass


if __name__ == '__main__':
    with HTTPServer(('', PORT), Handler) as server:
        print(f"HTTP Server Running on port {PORT}...")
        server.serve_forever()
