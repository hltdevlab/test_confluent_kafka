from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer


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
