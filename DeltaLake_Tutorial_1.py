from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import shutil

parquet_path = "/home/benoit/DeltaLake_Experiment/Data/loans_parquet"

spark = SparkSession \
        .builder \
        .appName("Python Spark create RDD example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

def Create_DataFile():

    # Delete a new parquet table with the parquet file
    if os.path.exists(parquet_path):
        print(parquet_path)
        shutil.rmtree(parquet_path)
    
    # Create a new parquet table with the parquet file
    spark.read.format("parquet").load("/home/benoit/DeltaLake_Experiment/Data/") \
    .write.format("parquet").save(parquet_path)
    print("Created a Parquet table at " + parquet_path)

spark.read.format("parquet").load(parquet_path).createOrReplaceTempView("loans_parquet")
print("Defined view 'loans_parquet'")

sqlDF = spark.sql("select * from loans_parquet")
sqlDF.show()

MyCatalog = spark.catalog
for table in spark.catalog.listDatabases():
    print(table)