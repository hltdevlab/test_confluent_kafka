from producers import PureAvroProducer

producer = PureAvroProducer(
    bootstrap_servers="localhost:9092",
    topic="topic1",
    avsc_path="User.avsc"
)

producer.send({"name": "tom", "age": 20, "email": None})
print("message sent")
