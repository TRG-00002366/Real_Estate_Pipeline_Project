from pyspark.sql import SparkSession
import json
def main():
    spark = SparkSession.builder \
            .appName("Real Estate Data Pipeline") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

    sc = spark.sparkContext

    # 3A

    df = spark.read.parquet("/opt/data/raw/listing_events")
    rdd = df.rdd
    parsed_rdd = rdd.map(lambda x: json.loads(x["message"]))
    
    rdd_rented = parsed_rdd.filter(lambda x: x["rental_status"] == "rented")
    rdd_rented.coalesce(1).saveAsTextFile('/opt/data/transformed/rented_listings')

    rdd_revenue = parsed_rdd.map(lambda x: (x["property_id"], x["rent"] * x["duration"])).reduceByKey(lambda x,y: x+y)
    rdd_revenue.coalesce(1).saveAsTextFile('/opt/data/transformed/revenue_per_property')

if __name__ == "__main__":
    main()