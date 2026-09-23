import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="kafka:9092",
    group_id="payment-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode()),
)

print("[ОПЛАТА] Старт, группа: payment-group", flush=True)

for msg in consumer:
    o = msg.value
    print(
        f"[ОПЛАТА] Заказ №{o['order_id']}: списание {o['amount']} ₽ "
        f"за «{o['item']}» (покупатель: {o['buyer']}), "
        f"партиция={msg.partition}",
        flush=True,
    )