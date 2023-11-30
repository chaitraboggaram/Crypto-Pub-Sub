import cgi
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from jsonrpclib import Server

PORT = 8020
sender_track = {}


class Sender(threading.Thread):
    def __init__(self, thread_name, topic_name, total_message_count):
        print("Server has been initiated!")
        self.topic_name = topic_name
        self.full_thread_name = "Producer: " + f"\"{thread_name}\"" + " for topic " + topic_name
        self.broker = Server('http://localhost:8010')
        self.total_message_count = total_message_count
        self.rate_tracker_url = "https://api.coincap.io/v2/assets/"
        threading.Thread.__init__(self, name=self.full_thread_name)
        self._stop_event = threading.Event()

    def run(self):
        msg_sent_count = 0
        while not self._stop_event.is_set():
            print(self.full_thread_name, "publishing", self.total_message_count,
                  "total messages with at an interval of 500ms per each message...")
            for _ in range(self.total_message_count):
                if not self._stop_event.is_set():
                    message = self.get_crypto_data()
                    print("Producer message content: ", message)
                    self.broker.publish(self.topic_name, message)
                    msg_sent_count += 1
                    time.sleep(0.5)
                    self._stop_event.wait(timeout=0.5)

            print(self.full_thread_name, msg_sent_count, "messages sent!")
            self.stop()

    def stop(self):
        self._stop_event.set()

    def get_crypto_data(self):
        response = requests.get(self.rate_tracker_url + self.topic_name)
        data = json.loads(response.text)['data']
        max_supply = round(float(data['maxSupply']), 5) if data['maxSupply'] else 'Unlimited'
        data = {
            "currency": data['symbol'],
            "price": round(float(data['priceUsd']), 5),
            "supply": round(float(data['supply']), 5),
            "maxSupply": max_supply
        }
        return data


class Handler(BaseHTTPRequestHandler):
    global sender_track

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        message = "Hello! Please use index.html to demo Pub-Sub for Crypto Prices!"
        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST'}
        )
        thread_name = form.getvalue("sender")
        topic_name = form.getvalue("topic_name")
        number_of_messages = int(form.getvalue("num_msg"))
        msg_type = form.getvalue("msg_type")
        self.send_response(200)
        self.end_headers()

        if msg_type == 'info':
            if topic_name in sender_track:
                sender_track[topic_name].stop()
                sender_track[topic_name].join()
            worker = Sender(thread_name, topic_name, number_of_messages)
            sender_track[topic_name] = worker
            worker.start()

        elif msg_type == 'end':
            # Skip ending if no sender_track
            if topic_name in sender_track:
                sender_track[topic_name].stop()
                sender_track[topic_name].join()
                del sender_track[topic_name]


if __name__ == "__main__":
    with HTTPServer(('', PORT), Handler) as server:
        print(f"HTTP Server Running on port {PORT}...")
        server.serve_forever()
