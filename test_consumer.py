from collections.abc import Callable
from confluent_kafka import Consumer
# from confluent_kafka.error import KeyDeserializationError, ValueDeserializationError
# from confluent_kafka.serialization import SerializationContext, MessageField
# from schema_registry_avro_utils import get_avro_deserializer
from confluent_kafka_utils.config import settings
from confluent_kafka_utils.serialization.AvroSchemaSerialization import AvroSchemaSerialization



bootstrap_servers = settings.bootstrap_servers
group_id = settings.group_id
schema_registry_url = settings.schema_registry_url
topic = settings.topic
key_avsc_path = settings.key_avsc_path
value_avsc_path = settings.value_avsc_path

# key_avro_deserializer = get_avro_deserializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
# value_avro_deserializer = get_avro_deserializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')

# def deserialize_data(message_field_key_value, topic, deserializer, data, headers=None):
#     ctx = SerializationContext(topic, message_field_key_value, headers)
#     if deserializer is not None:
#         deserialized_data = deserializer(data, ctx)
#         return deserialized_data

# def deserialize_key(topic, deserializer, key, headers=None):
#     try:
#         return deserialize_data(MessageField.KEY, topic, deserializer, key, headers)
#     except Exception as se:
#         raise KeyDeserializationError(se)

# def deserialize_value(topic, deserializer, value, headers=None):
#     try:
#         return deserialize_data(MessageField.VALUE, topic, deserializer, value, headers)
#     except Exception as se:
#         raise ValueDeserializationError(se)



avro_serialization = AvroSchemaSerialization(
    schema_registry_url=schema_registry_url,
    key_avsc_path=key_avsc_path,
    value_avsc_path=value_avsc_path
)




# conf = {
#     "bootstrap.servers": bootstrap_servers,
#     "key.serializer": key_avro_serializer,
#     "value.serializer": value_avro_serializer,
# }

conf = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id
}

consumer = Consumer(conf)

consumer.subscribe([topic])

def my_on_message(msg):
    # key = deserialize_key(msg.topic(), key_avro_deserializer, msg.key(), msg.headers())
    # value = deserialize_value(msg.topic(), value_avro_deserializer, msg.value(), msg.headers())
    key = avro_serialization.deserialize_key(msg.topic(), msg.key(), msg.headers())
    value = avro_serialization.deserialize_value(msg.topic(), msg.value(), msg.headers())
    print(f"{key} | {value}")


def consume():
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


def listen(on_message_fn:Callable=None):
    print("Consumer listening...")
    on_message_fn = on_message_fn

    while True:
        message = consume()
        if message is None:
            continue

        on_message_fn(message)


listen(
    # on_message_fn=lambda msg: print(f"{msg.key()} | {msg.value()}")
    on_message_fn=my_on_message
)
