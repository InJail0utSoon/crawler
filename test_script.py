from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
for i in range(100):
    producer.send('web-crawler-queue', b'some temp message ').get(timeout=10)
