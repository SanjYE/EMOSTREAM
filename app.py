from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import threading
import time

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500 
)

def periodic_flush():
    while True:
        time.sleep(0.5)
        producer.flush()

flush_thread = threading.Thread(target=periodic_flush, daemon=True)
flush_thread.start()

@app.route('/emoji', methods=['POST'])
def receive_emoji():
    data = request.get_json()
    required_fields = ['user_id', 'emoji_type', 'timestamp']

    if not data:
        return jsonify({'error': 'No data provided'}), 400

    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return jsonify({'error': f"Missing fields: {', '.join(missing_fields)}"}), 400

    producer.send('emoji_topic', value=data)

    return jsonify({'status': 'success'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
