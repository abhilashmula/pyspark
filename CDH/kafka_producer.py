from kafka import KafkaProducer


def produce():
    producer = KafkaProducer(bootstrap_servers='cdh.informatica.com:9092')
    for i in range(100):
        print "Sending message {}".format(i)
        producer.send('truckevent', str(i))
        producer.flush()

if __name__ == "__main__":
    produce()