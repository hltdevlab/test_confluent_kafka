import sys
from collections.abc import Callable
from confluent_kafka import Consumer
from confluent_kafka.error import SerializationError
from confluent_kafka_utils.config import settings
from confluent_kafka_utils.serialization.AvroSchemaSerialization import AvroSchemaSerialization



def my_on_message(msg):
    key = avro_serialization.deserialize_key(msg.topic(), msg.key(), msg.headers())
    value = avro_serialization.deserialize_value(msg.topic(), msg.value(), msg.headers())
    print(f"{key} | {value} | {msg.headers()}")



def consume(consumer:Consumer):
    try:
        message = consumer.poll(timeout=1.0)
        if message is None:
            return None
        if message.error():
            print(f"Consumer error: {message.error()}")
            return None
        return message
    except SerializationError as e:
        print(f"Consumer error: {e}")


def listen(consumer:Consumer, on_message_fn:Callable=None, topic=None, group_id=None):
    print(f"Consumer listening on '{topic}' as '{group_id}'...")
    on_message_fn = on_message_fn

    while True:
        message = consume(consumer)
        if message is None:
            continue

        on_message_fn(message)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(f"Script name: {sys.argv[0]}")
        print("Arguments received:")
        for i, arg in enumerate(sys.argv[1:]):
            print(f"  Argument {i+1}: {arg}")
    else:
        print("No arguments provided.")
    
    group_id = sys.argv[1] if len(sys.argv) > 1 else settings.group_id
        
    bootstrap_servers = settings.bootstrap_servers
    schema_registry_url = settings.schema_registry_url
    topic = settings.topics.get("fish_soup")
    key_avsc_paths = settings.key_avsc_paths
    value_avsc_paths = settings.value_avsc_paths
    
    avro_serialization = AvroSchemaSerialization(
        schema_registry_url=schema_registry_url,
        key_avsc_paths=key_avsc_paths,
        value_avsc_paths=value_avsc_paths
    )

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id
    }

    consumer = Consumer(conf)

    consumer.subscribe([topic])
    
    listen(
        consumer,
        topic=topic,
        group_id=group_id,
        # on_message_fn=lambda msg: print(f"{msg.key()} | {msg.value()}")
        on_message_fn=my_on_message
    )