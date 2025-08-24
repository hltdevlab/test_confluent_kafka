import time
from confluent_kafka import Producer
from confluent_kafka.error import KeySerializationError, ValueSerializationError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka_utils.config import settings


bootstrap_servers = settings.bootstrap_servers
schema_registry_url = settings.schema_registry_url
# topic = settings.topic
# key_avsc_path = settings.key_avsc_path
# value_avsc_path = settings.value_avsc_path

_producer = None


def __init():
    global _producer
    conf = {
        "bootstrap.servers": bootstrap_servers,
        # "queue.buffering.max.messages": 1
    }
    _producer = Producer(conf)
    print("Producer instantiated.")
    return _producer



def __get_producer():
    global _producer
    if _producer is None:
        print("Producer is None.")
        return __init()
    
    return _producer


def close_producer():
    global _producer

    if _producer is None:
        return
    
    _producer.flush(timeout=5)
    _producer = None
    print("Producer is destroyed.")


def __delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        # print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
        pass


def send(topic, key, value, avro_serialization, *args, **kwargs):
    headers = kwargs.get("headers", None)
    # print("--- sending ---")
    producer = __get_producer()
    
    serialized_key = avro_serialization.serialize_key(topic, key, headers=headers)
    serialized_value = avro_serialization.serialize_value(topic, value, headers=headers)

    # print(f"serialized_key: {serialized_key}")
    # print(f"serialized_value: {serialized_value}")

    for i in range(3):
        if i > 0:
            print(f"buffer_full_retry({i}) ...")
        
        try:
            producer.produce(
                *args,
                **kwargs,
                topic=topic,
                key=serialized_key,
                value=serialized_value,
                on_delivery=__delivery_report,
            )
            break
        except BufferError:
            wait_time = 0.1 * (i + 1)
            print(f"buffer full. pause for a while ({wait_time})...")
            time.sleep(wait_time)
            producer.poll(0)
