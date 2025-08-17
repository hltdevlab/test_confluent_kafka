from confluent_kafka_utils.config import settings
from confluent_kafka_utils.producer import close_producer
from send_utils import send_1, send_2

key = {"user_id": "tom_id"}
value = {"name": "tom", "age": 20, "email": None}
# send(settings.topic, key, value, avro2_serialization)
# send_2(settings.topic, key, value)

value = {"name": "tom", "age": 21, "email": None}
# send(settings.topic, key, value, avro1_serialization)
# send_1(settings.topic, key, value)

value = {"name": "tom", "age": 22, "email": None}
# send(settings.topic, key, value, avro2_serialization)
# send_2(settings.topic, key, value)

value = {"name": "tom", "age": 23, "email": None}
# send(settings.topic, key, value, avro1_serialization)
# send_1(settings.topic, key, value)

value = {"name": "tom", "age": 24, "email": None}
# send(settings.topic, key, value, avro2_serialization)
# send_2(settings.topic, key, value)


close_producer()


try:
    for i in range(2000000):
        key = {"user_id": f"uid{i}"}
        value = {"name": f"u{i}", "age": i, "email": None}
        send_1(settings.topic, key, value)

except Exception as e:
    print(e)
finally:
    close_producer()
