import os
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.appName("smoke").getOrCreate()
sc = spark.sparkContext


def versions(_):
    import sys

    import h3
    import pyarrow

    yield (sys.version.split()[0], h3.__version__, pyarrow.__version__)


import h3
import pyarrow

driver = (sys.version.split()[0], h3.__version__, pyarrow.__version__)
workers = set(sc.parallelize(range(8), 8).mapPartitions(versions).collect())
assert workers == {driver}, f"driver {driver} != executors {workers}"
print(f"python/h3/pyarrow identical on driver and executors: {driver}")


@pandas_udf("long") # type: ignore
def h3_r5(lat: pd.Series, lng: pd.Series) -> pd.Series:
    return pd.Series([int(h3.latlng_to_cell(a, b, 5), 16) for a, b in zip(lat, lng)])


df = spark.range(1000).select(
    "id",
    ((F.col("id") % 179) - 89).cast("double").alias("lat"),
    (((F.col("id") * 7) % 359) - 179).cast("double").alias("lng"),
)
df = df.withColumn("h3_r5", h3_r5("lat", "lng")) # type: ignore
df.write.mode("overwrite").parquet("hdfs:///tmp/smoke")

back = spark.read.parquet("hdfs:///tmp/smoke")
assert back.count() == 1000
assert back.filter(F.col("h3_r5").isNull()).count() == 0

hadoop = spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
print(f"spark {spark.version} (expected {os.environ['SPARK_VERSION']}), hadoop client {hadoop}")
assert spark.version == os.environ["SPARK_VERSION"]
print("spark smoke OK")
spark.stop()
