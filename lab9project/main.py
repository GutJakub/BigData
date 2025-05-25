from pyspark.sql import SparkSession
from src.pipeline.utils import ReadFile, TransformData, SaveData
import time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SimplePipeline").getOrCreate()

    local_path = "data/yellow_tripdata_2023-01.parquet"

    df = ReadFile(spark, local_path)
    t1 = time.time()
    count = df.count()
    t = time.time() - t1
    print(f"Number of records: {count}, Total time: {t:.2f} seconds")
    t1 = time.time()
    count = df.count()
    t = time.time() - t1
    print(f"Number of records: {count}, Total time after cache(): {t:.2f} seconds")
    df = TransformData(df)
    print(f"After the filter of payment_type==2: {df.count()}")
    df.show(10)
    output = "output/"
    SaveData(df, output, w_mode = "overwrite", num_partitions=4)
    #df_transformed = transform_data(df)
    #save_data(df_transformed, output_path)

    spark.stop()