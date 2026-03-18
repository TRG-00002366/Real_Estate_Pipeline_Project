from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
            .appName("Real Estate Data Pipeline") \
            .master("local[*]") \
            .getOrCreate()

if __name__ == "__main__":
    main()