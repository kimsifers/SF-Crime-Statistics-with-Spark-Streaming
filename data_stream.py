import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf





schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", TimestampType(), True),
                     StructField("call_date", DateType(), True),
                     StructField("offense_date", TimestampType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)])

def run_spark_job(spark):
    
    
    
    #References for software development and performance tuning not including Udacity 
    # https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    # https://sparkbyexamples.com/pyspark/pyspark-cast-column-type/ 
    # https://spark.apache.org/docs/latest/monitoring 
    # https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/ProgressReporter.scala
    # http://canali.web.cern.ch/docs/Spark_Summit_2017EU_Performance_Luca_Canali_CERN.pdf
    #https://www.linkedin.com/pulse/monitoring-apache-spark-streaming-understanding-key-ramachandra 
    #https://sparkbyexamples.com/spark/spark-read-json-from-multiline/
    #https://spark.apache.org/docs/3.1.1/configuration.html#spark-streaming
    
    
    # .option("maxOffsetsPerTrigger", 200) \ , was used in experiments 
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sf_police_dept_calls_kafka_server") \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    print("df print schema")
    df.printSchema()
    
    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string) value")
    
    print("kafka_df printSchema")
    kafka_df.printSchema()
    

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")
    
    print("service_table printSchema")
    service_table.printSchema()
    
     # TODO select original_crime_type_name and disposition
    distinct_table = service_table\
        .select("original_crime_type_name","disposition")
    
    print("distinct_table printSchema")
    distinct_table.printSchema()
    
    # count the number of original crime type
    df.printSchema()
    agg_df = (
        distinct_table\
            .groupBy("original_crime_type_name")
            .count()
    )
    print("agg_df printSchema")
    agg_df.printSchema()
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    # .trigger(processingTime= "0 seconds" ) \ , was used in experiments 
 
    
    
    query = (
        agg_df
            .writeStream \
            .format ("console") \
            .queryName("dispostiongroupbyorginalcrimetypename") \
            .outputMode("complete") \
            .start()
    )
    
    print("print query")
    print(query)
    query.awaitTermination()
    
    # TODO get the right radio code json path
    #radio_code_json_filepath = ""
    #radio_code_df = spark.read.json(radio_code_json_filepath)
    
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)
    
    print("radio_code print scheme")
    radio_code_df.printSchema()
    print("radio_code show")
    radio_code_df.show()
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    
    # TODO join on disposition column
    #join_query = agg_df 
    join_query = agg_df.join(radio_code_df, agg_df.disposition == radio_code_df.disposition,"inner")
    
    print("print join_query")
    print(join_query)

    
    join_query.awaitTermination()

    
    

  
    
    
   
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    


    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
