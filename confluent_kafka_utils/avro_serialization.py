from confluent_kafka_utils.config import settings
from confluent_kafka_utils.serialization.AvroSchemaSerialization import AvroSchemaSerialization


test_settings = {
    "avro_schemas": {
        "set1": {
            "key_avsc_paths": settings.key_avsc_paths,
            "value_avsc_paths": settings.value_avsc_paths
        },
        "set2": {
            "key_avsc_paths": settings.key_avsc_paths,
            "value_avsc_paths": settings.value_avsc_paths
        }
    }
}

_avro_serialization = {}

def get_avro_serialization(avro_schema_name):
    if avro_schema_name in _avro_serialization:
        return _avro_serialization.get(avro_schema_name)

    avro_schema = test_settings.get("avro_schemas", {}).get(avro_schema_name, {})
    key_avsc_paths = avro_schema.get("key_avsc_paths", [])
    value_avsc_paths = avro_schema.get("value_avsc_paths", [])

    _avro_serialization[avro_schema_name] = AvroSchemaSerialization(
        schema_registry_url=settings.schema_registry_url,
        key_avsc_paths=key_avsc_paths,
        value_avsc_paths=value_avsc_paths
    )
    return _avro_serialization.get(avro_schema_name)


avro_schemas = test_settings.get("avro_schemas", {})
for avro_schema_name in avro_schemas:
    get_avro_serialization(avro_schema_name)


# avro1_serialization = AvroSchemaSerialization(
#     schema_registry_url=settings.schema_registry_url,
#     key_avsc_path=settings.key_avsc_path,
#     value_avsc_path=settings.value_avsc_path
# )

# avro2_serialization = AvroSchemaSerialization(
#     schema_registry_url=settings.schema_registry_url,
#     key_avsc_path=settings.key_avsc_path,
#     value_avsc_path=settings.value_avsc_path
# )
