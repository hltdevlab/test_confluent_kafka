from confluent_kafka import Producer
from confluent_kafka.error import KeySerializationError, ValueSerializationError
from confluent_kafka.serialization import SerializationContext, MessageField
from schema_registry_avro_utils import get_avro_serializer, read_schema
from config import settings
from serialization.AvroSchemaSerialization import AvroSchemaSerialization


bootstrap_servers = settings.bootstrap_servers
schema_registry_url = settings.schema_registry_url
topic = settings.topic
key_avsc_path = settings.key_avsc_path
value_avsc_path = settings.value_avsc_path

key_avro_serializer = get_avro_serializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
value_avro_serializer = get_avro_serializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')


def serialize_data(message_field_key_value, topic, serializer, data, headers=None):
    ctx = SerializationContext(topic, message_field_key_value, headers)
    if serializer is not None:
        serialized_data = serializer(data, ctx)
        return serialized_data

def serialize_key(topic, serializer, key, headers=None):
    # ctx = SerializationContext(topic, MessageField.KEY, headers)
    # if key_serializer is not None:
    #     try:
    #         serialized_key = key_serializer(key, ctx)
    #         return serialized_key
    #     except Exception as se:
    #         raise KeySerializationError(se)
    try:
        return serialize_data(MessageField.KEY, topic, serializer, key, headers)
    except Exception as se:
        raise KeySerializationError(se)

def serialize_value(topic, serializer, value, headers=None):
    # ctx = SerializationContext(topic, MessageField.VALUE, headers)
    # if value_serializer is not None:
    #     try:
    #         serialized_value = value_serializer(value, ctx)
    #         return serialized_value
    #     except Exception as se:
    #         raise ValueSerializationError(se)
    try:
        return serialize_data(MessageField.VALUE, topic, serializer, value, headers)
    except Exception as se:
        raise ValueSerializationError(se)

# conf = {
#     "bootstrap.servers": bootstrap_servers,
#     "key.serializer": key_avro_serializer,
#     "value.serializer": value_avro_serializer,
# }


key = {"user_id": "tom_id"}
value = {"name": "tom", "age": 20, "email": None}


# serialized_key = serialize_key(topic, key_avro_serializer, key)
# serialized_value = serialize_value(topic, value_avro_serializer, value)


avro_serialization = AvroSchemaSerialization(
    schema_registry_url=schema_registry_url,
    key_avsc_path=key_avsc_path,
    value_avsc_path=value_avsc_path
)

serialized_key = avro_serialization.serialize_key(topic, key)
serialized_value = avro_serialization.serialize_value(topic, value)

print(f"serialized_key: {serialized_key}")
print(f"serialized_value: {serialized_value}")

conf = {
    "bootstrap.servers": bootstrap_servers
}

producer = Producer(conf)
producer.produce(
    topic=topic,
    key=serialized_key,
    value=serialized_value,
)
producer.flush()
