from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

def ReadFile(spark: SparkSession,local_path: str):
    df = spark.read.parquet(local_path).limit(10000).cache()
    return df
def TransformData(df: DataFrame):
    df = df.withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    return df.filter(col("payment_type")==2)

def SaveData(df: DataFrame, local_path: str, w_mode: str, num_partitions: int):
    (df
     .coalesce(num_partitions)
     .write
     .mode(w_mode)
     .parquet(local_path))
#from pipeline import utils
#print(utils.ReadFile)
#<function ReadFile at 0x00000216CF743D90>
#