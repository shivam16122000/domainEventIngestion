from flask import Flask, jsonify
import threading
import random
import datetime
import time

# Create the Flask app
app = Flask(__name__)

# Define an endpoint that returns a JSON order event
@app.route('/order', methods=['GET'])
def order_event():
    order_id = random.randint(1000, 9999)
    event = {
        "order_id": order_id,
        "event_time": datetime.datetime.now().isoformat(),
        "amount": round(random.uniform(10, 500), 2),
        "status": random.choice(["pending", "completed", "failed"])
    }
    return jsonify(event)

# Function to run the Flask app
def run_app():
    # Use host '0.0.0.0' so it is reachable on your local network if needed.
    app.run(host='0.0.0.0', port=3004, debug=False)

# Start the Flask app in a background thread
flask_thread = threading.Thread(target=run_app)
flask_thread.setDaemon(True)  # This ensures the thread will exit when the main program exits.
flask_thread.start()

# Allow the server a moment to start
time.sleep(1)

# Test the API endpoint
import requests
response = requests.get("http://localhost:3004/order")
print("API Response:", response.json())
