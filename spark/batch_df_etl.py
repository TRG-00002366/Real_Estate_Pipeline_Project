from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, lit, when, count, sum, avg, substring, from_json

def main():
    spark = SparkSession.builder \
            .appName("Real Estate Data Pipeline") \
            .master("local[*]") \
            .getOrCreate()

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

    df_string = spark.read.parquet("/opt/data/raw")
    df_parsed = df_string.select(from_json(df_string.message, schema).alias("data"))
    df = df_parsed.select("data.*")
    df.cache() # TODO add parttioning/bucketing
    df.count()


    # Hourly Rentals Summary
    hourly_rentals = df.withColumn('revenue', col('rent') * col('duration')).withColumn('rented_hour', substring(col('rented_on'), 0, 13)).groupBy(col('rented_hour')).agg(
        count('*').alias('total_rentals'),
        sum('revenue').alias('total_revenue'),
        avg('revenue').alias('average_rental_value')
    )
    hourly_rentals.write.mode("overwrite").parquet('/opt/data/hourly_rentals')


    # Rentals by Bedroom Count
    window_partition = Window.partitionBy("bedrooms")
    rentals_by_bedrooms = df.withColumn('rentals_by_bedrooms', count(col('property_id')).over(window_partition))
    rentals_by_bedrooms.write.mode("overwrite").parquet('/opt/data/rentals_by_bedrooms')


    # State Revenue
    city_state_schema = StructType([
        StructField('city', StringType(), False),
        StructField('state', StringType(), False),
    ])

    states = spark.read.csv(
        '/opt/data/states.csv',
        header=True,
        schema=city_state_schema
    )

    df.createOrReplaceTempView('listings')
    states.createOrReplaceTempView('states')

    revenue_by_state = spark.sql('SELECT s.state, SUM(l.rent*l.duration) AS revenue FROM (listings l INNER JOIN states s ON l.city = s.city) GROUP BY s.state')
    revenue_by_state.write.mode("overwrite").parquet('/opt/data/revenue_by_state')


    # Rental Status Breakdown
    status_breakdown = df.groupBy('building_type').pivot('rental_status', ['rented', 'open']).agg(count('rental_status')).fillna(0)
    status_breakdown.write.mode("overwrite").parquet('/opt/data/status_breakdown')

if __name__ == "__main__":
    main()