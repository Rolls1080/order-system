import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="kafka:9092",
    group_id="delivery-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode()),
)

print("[ДОСТАВКА] Старт, группа: delivery-group", flush=True)

for msg in consumer:
    o = msg.value
    print(
        f"[ДОСТАВКА] Заказ №{o['order_id']}: подготовка доставки "
        f"«{o['item']}» покупателю {o['buyer']}, "
        f"партиция={msg.partition}",
        flush=True,
    )