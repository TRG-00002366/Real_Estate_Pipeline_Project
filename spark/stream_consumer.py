from pyspark.sql import SparkSession
import argparse

def main():

    #parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration",type=int,default=30)
    parser.add_argument("--bootstrap-servers",type=str,default='kafka:9092')
    args = parser.parse_args()
    duration= args.duration

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Real Estate Data Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("startingOffsets", "earliest") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "listing-events") \
        .load()

    # Kafka messages have key, value, topic, partition, offset, timestamp
    # The value column contains the actual message (as bytes)
    messages = kafka_df.selectExpr("CAST(value AS STRING) as message")

    # Write to console (for demonstration)
    query = messages.writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/opt/data/raw/listing_events") \
        .option("checkpointLocation", "/opt/data/tmp/checkpoint") \
        .start()


    query.awaitTermination(duration)
    query.stop()
    spark.stop()

if __name__ == "__main__":
    main()