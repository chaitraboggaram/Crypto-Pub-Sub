# Crypto Price Notifier

**Application Components:**
- **Broker**
- **Client (Subscriber)**
- **Server (Publisher)**

**Pre-Requisites:**
```bash
pip install -r requirements.txt
```

**Run the required Services:**
From directory of the project run the following: 

1. Run the Broker:
    ```bash
    python pubsub.py
    ```  

2. Run the Server:
    ```bash
    python pubsubServer.py
    ```

3. Run the Client:
    ```bash
    python pubsubClient.py
    ```

**Web Interface:**
- Open `index.html` in any browser locally. It's a static page and doesn't need to be served from a server.

**API Testing:**
1. Open Postman or any API testing platform.
2. Create a POST request to `http://localhost:5500`.
3. In the request body (form-data), include the following fields:
    - `sender` (any string value)
    - `channel_name` (bitcoin, ethereum, xrp, cardano or dogecoin)
    - `num_msg` (any integer value - will query coincap and publish the message these many times)
    - `msg_type` ('info' to start sending messages or 'end' to stop sending messages)
4. Fire the request.

**Web Notifications:**
- On the `index.html` webpage, subscribe to bitcoin, ethereum, xrp, cardano and/or dogecoin (based on the POST request).
- Observe the notifications coming through.

**(Optional) Testing with Kafka**

***Pre-Requisites***
- Install Kafka: (Steps from: https://kafka.apache.org/quickstart#quickstart_download)
- Starting Kafka services: (Steps from: https://kafka.apache.org/quickstart#quickstart_startserver)
  - Start ZooKeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`
  - Start Kafka: `bin/kafka-server-start.sh config/server.properties`

***Run the required Services***
1. Run the Broker to use Kafka as message broker:
    ```bash
    python pubsub.py --useKafka 
    ```  

2. Run the Server:
    ```bash
    python pubsubServer.py
    ```

3. Run the Client:
    ```bash
    python pubsubClient.py
    ```

_TIP: Install Kafka Jetbrains plugin to verify messages in queue within PyCharm: https://plugins.jetbrains.com/plugin/21704-kafka_