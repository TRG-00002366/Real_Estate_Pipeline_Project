from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
            .appName("Real Estate Data Pipeline") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

    sc = spark.sparkContext

    # 3A

    df = spark.read.parquet("/opt/data/raw")
    rdd = df.rdd
    # rdd.coalesce(1).saveAsTextFile('/opt/data/all_data_test')

    rdd_rented = rdd.filter(lambda x: x[10] == 'rented')
    rdd_rented.coalesce(1).saveAsTextFile('/opt/data/rented_listings')

    rdd_revenue = rdd.map(lambda x: (x[0], x[8]*x[9])).reduceByKey(lambda x,y: x+y)
    rdd_revenue.coalesce(1).saveAsTextFile('/opt/data/revenue_per_property')

if __name__ == "__main__":
    main()