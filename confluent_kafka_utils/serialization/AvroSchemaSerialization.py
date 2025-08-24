import json
from fastavro.schema import load_schema_ordered
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema, SchemaReference
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
# from schema_registry_avro_utils import get_avro_serializer
from confluent_kafka.error import KeySerializationError, ValueSerializationError, KeyDeserializationError, ValueDeserializationError

def read_schema(avsc_path):
        with open(avsc_path, "r") as f:
            schema_str = f.read()
            return schema_str

def _del_key_from_dict(obj, key):
    try:
        del obj[key]
    except:
        pass
        
def load_all_schema(avsc_paths):
    # schemas_str = [read_schema(avsc_path) for avsc_path in avsc_paths]
    schemas = load_schema_ordered(avsc_paths)
    _del_key_from_dict(schemas, "__fastavro_parsed")
    _del_key_from_dict(schemas, "__named_schemas")
    return json.dumps(schemas)


# def _get_field_type(field_type: str|list):
#     if type(field_type) is list:
#         return field_type[1] if len(field_type) >= 2 else None
#     # str
#     return field_type


# def _is_referrence(field: dict):
#     name = field.get("name", None)
#     field_type = field.get("type", None)
#     field_type = _get_field_type(field_type)
#     if "." in field_type:
#         # is referrence
#         return True
#     return False


# def _has_references(schema_dict: dict):
#     # schema_dict = json.loads(schema_str)
#     fields = schema_dict.get("fields", [])
#     for field in fields:
#         if _is_referrence(field):
#             return True
#     return False


# def _get_fields(schema_str: str):
#     schema_dict = json.loads(schema_str)
#     fields = schema_dict.get("fields", [])


# def _get_full_name(schema_dict: dict):
#     namespace = schema_dict.get("namespace", None)
#     name = schema_dict.get("name", None)
#     full_name = f"{namespace}.{name}"
#     return full_name


# def register(schema_registry_url: str, subject_name : str, avsc_paths: list):
#     sr_client = SchemaRegistryClient({"url": schema_registry_url})
#     schemas_str = [read_schema(avsc_path) for avsc_path in avsc_paths]
#     schemas_str_dict_tuple = [(schema_str, json.loads(schema_str)) for schema_str in schemas_str]

#     has_ref_schemas_str_dict_tuple = [(schema_str, schema_dict) for (schema_str, schema_dict) in schemas_str_dict_tuple if _has_references(schema_dict)]
#     no_ref_schemas_str_dict_tuple = [(schema_str, schema_dict) for (schema_str, schema_dict) in schemas_str_dict_tuple if not _has_references(schema_dict)]

#     no_ref_full_name_schema_mapping = {
#         _get_full_name(schema_dict): Schema(
#             schema_str=schema_str,
#             schema_type="AVRO"
#         )  for (schema_str, schema_dict) in no_ref_schemas_str_dict_tuple
#     }

#     # no_ref_schema_id = [no_ref_schema for no_ref_schema in no_ref_schemas]
#     for (full_name, schema) in no_ref_full_name_schema_mapping.items():
#         schema_id = sr_client.register_schema(
#             subject_name=full_name,
#             schema=schema
#         )
#         print(f"schema_id: {schema_id}")
    
#     SchemaReference(
#         name="",
#         subject=""
#     )

#     # for schema_str in schemas_str:
#     #     schema = _schema_str_to_schema(schema_str)

#     print()
    

def get_avro_serializer(schema_registry_url, avsc_paths):
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    # schemas_str = [read_schema(avsc_path) for avsc_path in avsc_paths]
    # schema_str = read_schema(avsc_paths)
    schema_str = load_all_schema(avsc_paths)
    avro_serializer = AvroSerializer(sr_client, schema_str)
    return avro_serializer


def get_avro_deserializer(schema_registry_url, avsc_paths):
    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    # schema_str = read_schema(avsc_paths)
    schema_str = load_all_schema(avsc_paths)
    avro_deserializer = AvroDeserializer(
        sr_client,
        schema_str,
        lambda obj, ctx: obj  # Convert Avro record to dict
    )
    return avro_deserializer


class AvroSchemaSerialization:
    def __init__(self, schema_registry_url, key_avsc_paths, value_avsc_paths):
        print("AvroSchemaSerialization instantiated")
        
        # testing register
        # register(schema_registry_url, "topic1", value_avsc_paths)


        self.key_avro_serializer = get_avro_serializer(schema_registry_url, key_avsc_paths) if key_avsc_paths else StringSerializer('utf_8')
        self.value_avro_serializer = get_avro_serializer(schema_registry_url, value_avsc_paths) if value_avsc_paths else StringSerializer('utf_8')
        
        self.key_avro_deserializer = get_avro_deserializer(schema_registry_url, key_avsc_paths) if key_avsc_paths else StringSerializer('utf_8')
        self.value_avro_deserializer = get_avro_deserializer(schema_registry_url, value_avsc_paths) if value_avsc_paths else StringSerializer('utf_8')

    

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