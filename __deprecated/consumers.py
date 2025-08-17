from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationError
from collections.abc import Callable
from schema_registry_avro_utils import get_avro_deserializer





class SchemaRegistryAvroConsumer:

    @staticmethod
    def __default_on_message_fn(message):
        # if message is not None:
        print(f"Received: Key: {message.key()} | Value: {message.value()}")
    

    def __init__(self, bootstrap_servers, group_id, topic, schema_registry_url, key_avsc_path=None, value_avsc_path=None):
        self.topic = topic

        # with open(avsc_path, "r") as f:
        #     schema_str = f.read()

        # sr_client = SchemaRegistryClient({"url": schema_registry_url})

        # self.avro_deserializer = AvroDeserializer(
        #     sr_client,
        #     schema_str,
        #     lambda obj, ctx: obj  # Convert Avro record to dict
        # )

        # self.avro_deserializer = get_avro_deserializer(schema_registry_url, key_avsc_path)

        # consumer_conf = {
        #     'bootstrap.servers': bootstrap_servers,
        #     'group.id': group_id,
        #     'key.deserializer': StringDeserializer('utf_8'),
        #     'value.deserializer': self.avro_deserializer,
        #     'auto.offset.reset': 'earliest'
        # }

        key_avro_deserializer = get_avro_deserializer(schema_registry_url, key_avsc_path) if key_avsc_path else StringSerializer('utf_8')
        value_avro_deserializer = get_avro_deserializer(schema_registry_url, value_avsc_path) if value_avsc_path else StringSerializer('utf_8')

        conf = {
            "bootstrap.servers": bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            "key.deserializer": key_avro_deserializer,
            "value.deserializer": value_avro_deserializer
        }

        self.consumer = DeserializingConsumer(conf)
        self.consumer.subscribe([self.topic])


    def consume(self):
        try:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                return None
            if message.error():
                print(f"Consumer error: {message.error()}")
                return None
            return message
        except SerializationError as e:
            print(f"Consumer error: {e}")
    

    def listen(self, on_message_fn:Callable=None):
        print("Consumer listening...")
        on_message_fn = on_message_fn if on_message_fn else SchemaRegistryAvroConsumer.__default_on_message_fn

        while True:
            message = self.consume()
            if message is None:
                continue

            on_message_fn(message)

    def close(self):
        self.consumer.close()
