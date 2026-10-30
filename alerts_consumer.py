"""Крок 5. Читає temperature_alerts і humidity_alerts та виводить сповіщення.
Зупинка — Ctrl+C.
"""
import json

from kafka import KafkaConsumer

from configs import (MY_NAME, TOPIC_HUMIDITY_ALERTS, TOPIC_TEMPERATURE_ALERTS,
                     connection_params)

consumer = KafkaConsumer(
    TOPIC_TEMPERATURE_ALERTS,
    TOPIC_HUMIDITY_ALERTS,
    **connection_params(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: v.decode("utf-8") if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alerts_consumer",
)

print(f"Listening to '{TOPIC_TEMPERATURE_ALERTS}' and '{TOPIC_HUMIDITY_ALERTS}'...")

try:
    for message in consumer:
        alert = message.value
        kind = "TEMPERATURE" if message.topic == TOPIC_TEMPERATURE_ALERTS else "HUMIDITY"
        print(f"[{kind} ALERT] sensor {alert['sensor_id']} at {alert['timestamp']}: "
              f"{alert['message']}")
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
