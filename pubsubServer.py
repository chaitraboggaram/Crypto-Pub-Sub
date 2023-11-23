from http.server import BaseHTTPRequestHandler, HTTPServer
from jsonrpclib import Server
import time
import threading
import cgi
import random

class Sender(threading.Thread):
    def __init__(self, thread_name, channel_name, full_thread_name, nb_of_messages_to_send):
        print("Server has started!")
        threading.Thread.__init__(self, name=full_thread_name)
        self.thread_name = thread_name
        self.channel_name = channel_name
        self.full_thread_name = full_thread_name
        self.broker = Server('http://localhost:1006')
        self.nb_of_messages_to_send = nb_of_messages_to_send

    def run(self):
        print(self.full_thread_name, "Run start, sending",
              self.nb_of_messages_to_send,
              "messages with a pause of 50ms between each one...")
        for _ in range(self.nb_of_messages_to_send):
            message = self.get_crypto_data()
            print("message is this", message)
            self.broker.publish(self.channel_name, message)
            time.sleep(0.05)

        print(self.full_thread_name, self.nb_of_messages_to_send,
              "messages sent.")

    def get_crypto_data(self):
        # Simulate cryptocurrency price data
        price = random.uniform(1000, 50000)
        data = {
            "currency": self.channel_name,
            "price": round(price, 2)
        }
        return data

class Handler(BaseHTTPRequestHandler):
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
        self.send_response(200)
        self.end_headers()
        full_thread_name = "Sender: " + thread_name + " on " + channel_name
        worker = Sender(thread_name, channel_name, full_thread_name, number_of_messages)
        worker.start()

if __name__ == "__main__":
    with HTTPServer(('', 5500), Handler) as server:
        print("HTTP Server Running on port 5500...")
        server.serve_forever()
