### 2 producers, same interface
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from schema_registry_avro_utils import get_avro_serializer
import fastavro
from io import BytesIO
import json

class BaseProducer:
    def send(self, message: dict):
        raise NotImplementedError


class PureAvroProducer(BaseProducer):
    def __init__(self, bootstrap_servers, topic, avsc_path):
        self.topic = topic
        with open(avsc_path, "r") as f:
            self.schema = json.load(f)
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})

    def send(self, message: dict):
        buf = BytesIO()
        fastavro.schemaless_writer(buf, self.schema, message)
        self.producer.produce(self.topic, buf.getvalue())
        self.producer.flush()


class SchemaRegistryAvroProducer(BaseProducer):    

    def __init__(self, bootstrap_servers, topic, schema_registry_url, key_avsc_path=None, value_avsc_path=None):
        self.topic = topic

        key_avro_serializer = get_avro_serializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
        value_avro_serializer = get_avro_serializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "key.serializer": key_avro_serializer,
            "value.serializer": value_avro_serializer,
        }

        self.producer = SerializingProducer(conf)
    

    def send(self, key: dict, value: dict):
        self.producer.produce(topic=self.topic, key=key, value=value)
        self.producer.flush()
