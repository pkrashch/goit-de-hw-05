"""Кроки 3–4. Читає building_sensors, фільтрує дані та відправляє сповіщення
в temperature_alerts і humidity_alerts. Зупинка — Ctrl+C.
"""
import json

from kafka import KafkaConsumer, KafkaProducer

from configs import (MY_NAME, TOPIC_HUMIDITY_ALERTS, TOPIC_SENSORS,
                     TOPIC_TEMPERATURE_ALERTS, connection_params)

TEMPERATURE_MAX = 40
HUMIDITY_MAX = 80
HUMIDITY_MIN = 20

consumer = KafkaConsumer(
    TOPIC_SENSORS,
    **connection_params(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: v.decode("utf-8") if v else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"{MY_NAME}_alert_processor",
)

producer = KafkaProducer(
    **connection_params(),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: str(v).encode("utf-8"),
)


def check(data):
    """Повертає список сповіщень (topic, alert) для одного повідомлення датчика."""
    alerts = []
    temperature = data["temperature"]
    humidity = data["humidity"]

    if temperature > TEMPERATURE_MAX:
        alerts.append((TOPIC_TEMPERATURE_ALERTS, {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "temperature": temperature,
            "message": f"Temperature {temperature}°C exceeds threshold {TEMPERATURE_MAX}°C",
        }))

    if humidity > HUMIDITY_MAX or humidity < HUMIDITY_MIN:
        alerts.append((TOPIC_HUMIDITY_ALERTS, {
            "sensor_id": data["sensor_id"],
            "timestamp": data["timestamp"],
            "humidity": humidity,
            "message": f"Humidity {humidity}% is outside the allowed range "
                       f"{HUMIDITY_MIN}–{HUMIDITY_MAX}%",
        }))

    return alerts


print(f"Listening to '{TOPIC_SENSORS}'...")

try:
    for message in consumer:
        data = message.value
        print(f"Received: {data}")

        for topic, alert in check(data):
            producer.send(topic, key=alert["sensor_id"], value=alert)
            producer.flush()
            print(f"  -> ALERT sent to '{topic}': {alert['message']}")
except KeyboardInterrupt:
    print("\nProcessor stopped.")
finally:
    consumer.close()
    producer.close()
