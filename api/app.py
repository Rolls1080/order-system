import os, json
import mysql.connector
import pika
from kafka import KafkaProducer
from flask import Flask, request, jsonify

app = Flask(__name__)

# ← A добавляет сюда: producer = KafkaProducer(...)  (на самом деле это B)
# ← A добавляет сюда: def db(): ...
# ← B добавляет сюда: producer = KafkaProducer(...)
# ← B добавляет сюда: def publish_rabbit(...)

@app.post("/api/orders")
def create_order():
    data = request.get_json()
    # ← A: INSERT в MySQL
    # ← B: producer.send(...) и publish_rabbit(...)
    return jsonify({"order_id": order_id}), 201