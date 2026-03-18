from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
            .appName("Real Estate Data Pipeline") \
            .master("local[*]") \
            .getOrCreate()

    sc = spark.sparkContext

    # sample data - replace later
    data = [
        ('1', 'customer 1', 'Apartment', 2000, '2026-02-10T09:00:00Z', '2026-03-03T12:32:13Z', 1, 900, 2000, 12, 'rented', 'New York'),
        ('2', 'customer 2', 'Single Family', 1980, '2025-07-20T12:00:00Z', '2026-01-30T19:45:22Z', 3, 2350, 4000, 24, 'rented', 'Los Angeles'),
        ('3', 'customer 3', 'Apartment', 1970, '2025-10-31T06:17:55Z', None, 1, 680, 1500, 6, 'open', 'Chicago')
    ]

    # 3A

    rdd = sc.parallelize(data) # TODO replace with load from parquet 

    rdd_rented = rdd.filter(lambda x: x[10] == 'rented')
    # rdd_rented.saveAsTextFile('rented_listings')

    rdd_revenue = rdd.map(lambda x: (x[0], x[8]*x[9])).reduceByKey(lambda x,y: x+y)
    # rdd_revenue.saveAsTextFile('revenue_per_property')

if __name__ == "__main__":
    main()