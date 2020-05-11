from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Python Spark create RDD example") \
        .config("spark.some.config.option", "some-value") \
        .master("local[*]") \
        .getOrCreate()

def DataFrame_Exemple():
    df = spark.read.csv("/home/benoit/DeltaLake_Experiment/Data/MyData.csv",\
                        header=True,\
                        sep=';',\
                        inferSchema=True)
    
    df.show(5)
    df.printSchema()

DataFrame_Exemple()