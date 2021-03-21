import producer_server


def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"
    
    
    #https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
    # https://kafka-python.readthedocs.io/en/master/_modules/kafka/producer/kafka.html?highlight=batch
    #batch_size (int): Requests sent to brokers will contain multiple
    #            batches, one for each partition with data available to be sent.
    #            A small batch size will make batching less common and may reduce
    #            throughput (a batch size of zero will disable batching entirely).
    #            Default: 16384
    # https://stackoverflow.com/questions/65419339/kafka-producer-batch-size, Batch_size is in bytes 

    # TODO fill in blanks
    # naming covention: domain + source program 
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sf_police_dept_calls_kafka_server",
        bootstrap_servers="localhost:9092",
        client_id="sf_police_dept_calls",
        batch_size = 16384 
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
