from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, when, count

def main():
    spark = SparkSession.builder \
            .appName("City Ride Analytics") \
            .master("local[*]") \
            .getOrCreate()

    sc = spark.sparkContext

    # sample data - replace later
    data = [
        ('1', 'customer 1', 'Apartment', 2000, '2026-02-10', '2026-03-03', 1, 900, 2000, 12, 'rented', 'New York'),
        ('2', 'customer 2', 'Single Family', 1980, '2025-07-20', '2026-01-30', 3, 2350, 4000, 24, 'rented', 'Los Angeles'),
        ('3', 'customer 3', 'Apartment', 1970, '2025-10-31', None, 1, 680, 1500, 6, 'open', 'Chicago')
    ]

    # 3A

    rdd = sc.parallelize(data) # replace with load from parquet 

    rdd_rented = rdd.filter(lambda x: x[10] == 'rented')
    # rdd_rented.saveAsTextFile('rented_listings')

    rdd_revenue = rdd.map(lambda x: (x[0], x[8]*x[9])).reduceByKey(lambda x,y: x+y)
    # rdd_revenue.saveAsTextFile('revenue_per_property')

    # 3B

    schema = StructType([
        StructField('property_id', StringType(), False),
        StructField('customer_id', StringType(), False),
        StructField('building_type', StringType(), True),
        StructField('year_built', IntegerType(), True),
        StructField('posted_on', StringType(), True),
        StructField('rented_on', StringType(), True),
        StructField('bedrooms', IntegerType(), True),
        StructField('size', IntegerType(), True),
        StructField('rent', IntegerType(), True),
        StructField('duration', IntegerType(), True),
        StructField('rental_status', StringType(), True),
        StructField('city', StringType(), True),
    ])

    df = spark.createDataFrame(data, schema) # replace with load from parquet 

    # TODO Hourly Rentals Summary

    # Rentals by Bedroom Count
    window_partition = Window.partitionBy("bedrooms")
    rentals_by_bedrooms = df.withColumn('rentals_by_bedrooms', count(col('property_id')).over(window_partition))
    # rentals_by_bedrooms.show()

    # State Revenue
    city_state_schema = StructType([
        StructField('city', StringType(), False),
        StructField('state', StringType(), False),
    ])

    states = spark.read.csv(
        'states.csv',
        header=True,
        schema=city_state_schema
    )

    df.createOrReplaceTempView('listings')
    states.createOrReplaceTempView('states')

    revenue_by_state = spark.sql('SELECT s.state, SUM(l.rent*l.duration) AS revenue FROM (listings l INNER JOIN states s ON l.city = s.city) GROUP BY s.state')
    # revenue_by_state.show()

    # TODO Rental Status Breakdown


if __name__ == "__main__":
    main()