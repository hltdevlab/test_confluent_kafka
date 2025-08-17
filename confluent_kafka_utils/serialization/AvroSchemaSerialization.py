from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
# from schema_registry_avro_utils import get_avro_serializer
from confluent_kafka.error import KeySerializationError, ValueSerializationError, KeyDeserializationError, ValueDeserializationError


def read_schema(avsc_path):
        with open(avsc_path, "r") as f:
            schema_str = f.read()
            return schema_str
    

def get_avro_serializer(schema_registry_url, avsc_path):
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    schema_str = read_schema(avsc_path)
    avro_serializer = AvroSerializer(sr_client, schema_str)
    return avro_serializer


def get_avro_deserializer(schema_registry_url, avsc_path):
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    schema_str = read_schema(avsc_path)
    avro_deserializer = AvroDeserializer(
        sr_client,
        schema_str,
        lambda obj, ctx: obj  # Convert Avro record to dict
    )
    return avro_deserializer


class AvroSchemaSerialization:
    def __init__(self, schema_registry_url, key_avsc_path, value_avsc_path):
        print("AvroSchemaSerialization instantiated")
        self.key_avro_serializer = get_avro_serializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
        self.value_avro_serializer = get_avro_serializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')
        
        self.key_avro_deserializer = get_avro_deserializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
        self.value_avro_deserializer = get_avro_deserializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')

    

    def __process_data(self, message_field_key_value, topic, data_processor, data, headers=None):
        ctx = SerializationContext(topic, message_field_key_value, headers)
        if data_processor is not None:
            processed_data = data_processor(data, ctx)
            return processed_data


    def serialize_key(self, topic, key, headers=None):
        try:
            return self.__process_data(MessageField.KEY, topic, self.key_avro_serializer, key, headers)
        except Exception as se:
            raise KeySerializationError(se)

    def serialize_value(self, topic, value, headers=None):
        try:
            return self.__process_data(MessageField.VALUE, topic, self.value_avro_serializer, value, headers)
        except Exception as se:
            raise ValueSerializationError(se)
    
    def deserialize_key(self, topic, key, headers=None):
        try:
            return self.__process_data(MessageField.KEY, topic, self.key_avro_deserializer, key, headers)
        except Exception as se:
            raise KeyDeserializationError(se)

    def deserialize_value(self, topic, value, headers=None):
        try:
            return self.__process_data(MessageField.VALUE, topic, self.value_avro_deserializer, value, headers)
        except Exception as se:
            raise ValueDeserializationError(se)
        
    pass