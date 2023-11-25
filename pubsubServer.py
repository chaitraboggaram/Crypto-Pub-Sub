from http.server import BaseHTTPRequestHandler, HTTPServer
from jsonrpclib import Server
import time
import threading
import cgi
import random
import requests
import json

sender_track = {}
class Sender(threading.Thread):
    def __init__(self, thread_name, channel_name, full_thread_name, nb_of_messages_to_send):
        print("Server has started!")
        threading.Thread.__init__(self, name=full_thread_name)
        self.thread_name = thread_name
        self.channel_name = channel_name
        self.full_thread_name = full_thread_name
        self.broker = Server('http://localhost:5000')
        self.nb_of_messages_to_send = nb_of_messages_to_send
        self.rate_tracker_url = "https://api.coincap.io/v2/assets/"
        self._stop_event = threading.Event()

    def run(self):
        msg_sent_count = 0
        while not self._stop_event.is_set():
            print(self.full_thread_name, "Run start, sending",
                  self.nb_of_messages_to_send,
                  "messages with a pause of 50ms between each one...")
            for _ in range(self.nb_of_messages_to_send):
                if not self._stop_event.is_set():
                    message = self.get_crypto_data()
                    print("message is this", message)
                    self.broker.publish(self.channel_name, message)
                    msg_sent_count += 1
                    time.sleep(0.5)
                    self._stop_event.wait(timeout=0.5)

            print(self.full_thread_name, msg_sent_count, "messages sent.")
            self.stop()

    def stop(self):
        self._stop_event.set()

    def get_crypto_data(self):
        response = requests.get(self.rate_tracker_url + self.channel_name)
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

        message = "Hello, World!"
        self.wfile.write(bytes(message, "utf8"))

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={'REQUEST_METHOD': 'POST'}
        )
        thread_name = form.getvalue("sender")
        channel_name = form.getvalue("channel_name")
        number_of_messages = int(form.getvalue("num_msg"))
        msg_type = form.getvalue("msg_type")
        self.send_response(200)
        self.end_headers()
        full_thread_name = "Sender: " + thread_name + " on " + channel_name

        if msg_type == 'info':
            if channel_name in sender_track:
                sender_track[channel_name].stop()
                sender_track[channel_name].join()
            worker = Sender(thread_name, channel_name, full_thread_name, number_of_messages)
            sender_track[channel_name] = worker
            worker.start()

        elif msg_type == 'end':
            # Skip ending if no sender_track
            if channel_name in sender_track:
                sender_track[channel_name].stop()
                sender_track[channel_name].join()
                del sender_track[channel_name]

if __name__ == "__main__":
    with HTTPServer(('', 5500), Handler) as server:
        print("HTTP Server Running on port 5500...")
        server.serve_forever()
