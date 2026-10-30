"""Крок 1. Створення трьох топіків у Kafka."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import (MY_NAME, TOPIC_HUMIDITY_ALERTS, TOPIC_SENSORS,
                     TOPIC_TEMPERATURE_ALERTS, connection_params)

admin_client = KafkaAdminClient(**connection_params())

topics = [
    NewTopic(name=TOPIC_SENSORS, num_partitions=2, replication_factor=1),
    NewTopic(name=TOPIC_TEMPERATURE_ALERTS, num_partitions=1, replication_factor=1),
    NewTopic(name=TOPIC_HUMIDITY_ALERTS, num_partitions=1, replication_factor=1),
]

for topic in topics:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating '{topic.name}': {e}")

# Перевірка: виводимо лише свої топіки
print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
