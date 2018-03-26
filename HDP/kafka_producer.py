import  kafka
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['cdh.informatica.com:9092'])
topic = "truckevents"

producer.send(topic, b'test message')
