from config import settings
from consumers import SchemaRegistryAvroConsumer


consumer = SchemaRegistryAvroConsumer(
    bootstrap_servers=settings.bootstrap_servers,  # "localhost:9092",
    group_id=settings.group_id,  # "my-group",
    topic= settings.topic, # "topic1",
    schema_registry_url=settings.schema_registry_url,  # "http://localhost:8081",
    key_avsc_path=settings.key_avsc_path,
    value_avsc_path=settings.value_avsc_path
)

try:
    consumer.listen(
        # on_message_fn=lambda message: print(message.value())
    )

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
