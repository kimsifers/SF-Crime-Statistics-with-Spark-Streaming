from confluent_kafka import Consumer

#https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
# used the basic configuration and basic_consume_log 


conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "sf_police_dept_calls",
        'auto.offset.reset': "earliest"}

consumer = Consumer(conf)

topics =  ["sf_police_dept_calls_kafka_server"]

running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                #msg_process(msg)
                print(f"Successfully poll a record from "
                f"Kafka topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}\n"
                f"message value: {msg.value()}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False
    
if __name__ == "__main__":
    basic_consume_loop(consumer, topics)
    
