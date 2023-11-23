# Crypto News Notifier

**Application Components:**
- **Broker**
- **Client (Subscriber)**
- **Server (Publisher)**

**Installation:**
1. Run the Broker:
    ```bash
    python .\pubsub.py
    ```

2. Run the Server:
    ```bash
    python .\pubsubServer.py
    ```

3. Run the Client:
    ```bash
    python .\pubsubClient.py
    ```

**Web Interface:**
- Open `index.html` in any browser locally. It's a static page and doesn't need to be served from a server.

**API Testing:**
1. Open Postman or any API testing platform.
2. Create a POST request to `http://localhost:5500`.
3. In the request body (form-data), include the following fields:
    - `sender` (any string value)
    - `channel_name` (bitcoin or ethereum)
    - `num_msg` (any integer value)
    - `id` (any integer value)
    - `msg_type` (info or end)

4. Fire the request.

**Web Notifications:**
- On the `index.html` webpage, subscribe to bitcoin and/or ethereum (based on the POST request).
- Observe the notifications coming through.

