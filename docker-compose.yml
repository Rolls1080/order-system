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
        database=os.getenv("MYSQL_DATABASE",


"orders"),
    )

# ← B добавляет сюда: producer = KafkaProducer(...)
producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    key_serializer=lambda k: str(k).encode(),
    value_serializer=lambda v: json.dumps(v).encode(),
)

# ← B добавляет сюда: def publish_rabbit(...)
def publish_rabbit(event):
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(os.getenv("RABBIT_HOST", "rabbitmq"))
    )
    ch = conn.channel()
    ch.queue_declare(queue="order-notifications", durable=True)
    ch.basic_publish(
        exchange="",
        routing_key="order-notifications",
        body=json.dumps(event).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    conn.close()

"/api/orders"
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
    producer.send("order-events", key=order_id, value=event)
    producer.flush()
    publish_rabbit(event)
    return jsonify({"order_id": order_id}), 201