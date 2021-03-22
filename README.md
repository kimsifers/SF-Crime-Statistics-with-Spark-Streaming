# SF-Crime-Statistics-with-Spark-Streaming
For the Udacity Data Streaming Nano Degree 

Linux commands to run the San Franciso Crime Statistics project 


-- Start up Kafka zookeeper, separate terminal  
/usr/bin/zookeeper-server-start config/zookeeper.properties

-- Start up Kafka server separate terminal
/usr/bin/kafka-server-start /etc/kafka/server.properties


-- Start up Python modules in requirements.txt, separate terminal
./start.sh

-- Get police-department-calls-for-service.json and put into topic "sf_police_dept_calls_kafka_server"
python kafka_server.py 

-- test the  topic 
kafka-console-consumer --topic "sf_police_dept_calls_kafka_server" --bootstrap-server PLAINTEXT://localhost:9092 --from-beginning separate terminal

--  Kafka consumer of topic "sf_police_dept_calls_kafka_server"
python consumer_server.py 

-- spark submit data_stream.py: different command for spark, separate terminal
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py 

-- install a Web Browser on the Classroom server separate terminal
sudo apt install w3m

-- in the software logs found the Spark UI, it is equal to the Linux session name 
-- Successfully started service 'sparkDriver' on port 46881.
-- SparkUI:54 - Stopped Spark web UI at 
e.g. w3m http://c285a2c14ffa:4040

1.	How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
I read: 
•	Web UI - Spark 3.1.1 Documentation (apache.org)
•	https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/ProgressReporter.scala 
•	http://canali.web.cern.ch/docs/Spark_Summit_2017EU_Performance_Luca_Canali_CERN.pdf 
•	https://www.linkedin.com/pulse/monitoring-apache-spark-streaming-understanding-key-ramachandra 
I liked these definitions: 
•	Track the Latency of your Spark application. This is the time it takes to complete a single job on your Spark cluster
•	Measure the Throughput of your Spark application. This gives you the number of records that are being processed per second
In this project, we have learned the basics of performance tuning.  We are only using local memory.  In my experience communication latency causes the greatest variance.  I looked at the Job duration in Jobs tab and at the Stage Summary metrics. I found that Latency and Throughput correlated.  In the Stage Summary metrics, I could see some latency e.g Scheduler delay, GC Time.  Because we are using local memory, latency could influence Job duration. 
In my SF Crime Statistics with Spark Streaming Screen Shots, you can see I performed 7 experiments variating: producer_Server.PY batch_size,   data_stream.py maxOffsetsPerTrigger,  data_stream.py trigger(processingTime ) .  


2.	What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

I based my observations on Stage Summary metrics .  I understand Spark is built to be self-optimizing using it’s own default configurations. I observed that in the start of batch processing, performance was worse then improved with every batch.  The best results were obtained not configuring: producer_Server.PY batch_size,   data_stream.py maxOffsetsPerTrigger,  and data_stream.py trigger(processingTime ) .  The worst results were data_stream.py trigger(processingTime  = “60 seconds”), but by increasing data_stream.py maxOffsetsPerTrigger improved performance.  I tried setting producer_Server.PY batch_size = 35000, but this did not change performance.  It seems that Kafka offset options would also need to be changed to be effective. 

The Spark Monitoring and Instrumentation Web UI seems to be an excellent tool for starting performance tuning. In many use cases, it satisfies the needs for ensuring sql and configuration optimization.  It seems in more complex use cases, one would need to take advantage of Spark data export for performance optimization and monitoring. 
