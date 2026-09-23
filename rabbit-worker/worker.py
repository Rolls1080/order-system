import json
import time
import pika

# Ждём RabbitMQ до 60 секунд (30 попыток × 2 сек)
for attempt in range(30):
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
        break
    except pika.exceptions.AMQPConnectionError:
        print(f"[УВЕДОМЛЕНИЕ] Ожидание RabbitMQ... ({attempt+1}/30)", flush=True)
        time.sleep(2)
else:
    raise RuntimeError("RabbitMQ недоступен после 30 попыток")

ch = conn.channel()
ch.queue_declare(queue="order-notifications", durable=True)
ch.basic_qos(prefetch_count=1)


def handle(ch, method, props, body):
    o = json.loads(body)
    print(
        f"[УВЕДОМЛЕНИЕ] По заказу №{o['order_id']}: "
        f"{o['buyer']} оформил «{o['item']}» на сумму {o['amount']} ₽",
        flush=True,
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)


ch.basic_consume(queue="order-notifications", on_message_callback=handle)
print("[УВЕДОМЛЕНИЕ] Воркер запущен, ожидание сообщений", flush=True)
ch.start_consuming()