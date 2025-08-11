from config import settings
from producers import SchemaRegistryAvroProducer


producer = SchemaRegistryAvroProducer(
    bootstrap_servers=settings.bootstrap_servers,  # "localhost:9092",
    topic= settings.topic, # "topic1",
    schema_registry_url=settings.schema_registry_url,  # "http://localhost:8081",
    key_avsc_path=settings.key_avsc_path,
    value_avsc_path=settings.value_avsc_path
)


key = {"user_id": "tom_id"}
value = {"name": "tom", "age": 20, "email": None}

producer.send(key, value)
print("message sent")
