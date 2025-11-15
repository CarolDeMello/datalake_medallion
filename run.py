from pyspark.sql import SparkSession
from src.bronze import process_bronze
from src.silver import process_silver
from src.gold import process_gold

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkBeneficiarios") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    process_bronze(spark)
    process_silver(spark)
    process_gold(spark)

    spark.stop()