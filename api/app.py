import os, json
import mysql.connector
import pika
from kafka import KafkaProducer
from flask import Flask, request, jsonify

app = Flask(__name__)

# ← A добавляет сюда: producer = KafkaProducer(...)  (на самом деле это B)
# ← A добавляет сюда: def db(): ...
def db():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        user=os.getenv("MYSQL_USER", "app"),
        password=os.getenv("MYSQL_PASSWORD", "app"),
        database=os.getenv("MYSQL_DATABASE", "orders"),
    )

# ← B добавляет сюда: producer = KafkaProducer(...)
# ← B добавляет сюда: def publish_rabbit(...)

@app.post("/api/orders")
def create_order():
    data = request.get_json()
    # ← A: INSERT в MySQL
    buyer  = data["buyer"]
    item   = data["item"]
    amount = float(data["amount"])

    conn = db()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO orders (buyer, item, amount) VALUES (%s, %s, %s)",
        (buyer, item, amount),
    )
    conn.commit()
    order_id = cur.lastrowid
    cur.close()
    conn.close()

    event = {
        "order_id": order_id,
        "buyer": buyer,
        "item": item,
        "amount": amount,
    }
    # ← B: producer.send(...) и publish_rabbit(...)
    return jsonify({"order_id": order_id}), 201