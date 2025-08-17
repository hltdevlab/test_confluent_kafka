from confluent_kafka_utils.config import settings
from confluent_kafka_utils.producer import send
from confluent_kafka_utils.avro_serialization import get_avro_serialization


def send_1(topic, key, value):
    send(settings.topic, key, value, get_avro_serialization("set1"))

def send_2(topic, key, value):
    send(settings.topic, key, value, get_avro_serialization("set2"))
