from kafka import KafkaConsumer


def consume():
    consumer = KafkaConsumer('some-messages',
                             bootstrap_servers='cdh.informatica.com:9092')
    for msg in consumer:
        print "Got message {}".format(msg.value)

if __name__ == "__main__":
    consume()