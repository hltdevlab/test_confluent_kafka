import traceback
from confluent_kafka_utils.config import settings
from confluent_kafka_utils.producer import close_producer
from send_utils import send_1, send_2, send_message

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

foods = [key for (key, value) in settings.topics.items()]
topics = [value for (key, value) in settings.topics.items()]
for topic in topics:
    print(topic)

try:
    for i in range(1000000):
        food = foods[i % 3]
        topic = topics[i % 3]
        # key = {"user_id": f"uid{i}"}
        key = None
        value = {
            "name": food,
            # "quantity": (i % 10) + 1
            "quantity": i
        }
        headers = [("branch", f"{i % 2}")] if food == "fast_food" else None
        print(f"sending: {value}")
        send_message(topic, key, value, headers=headers)

except Exception as e:
    print(e)
    traceback.print_exc()
finally:
    close_producer()
