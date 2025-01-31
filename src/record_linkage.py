import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pseudopeople as pseudo

print("Loading Spark")
spark = SparkSession.builder.appName("Record Linkage").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()

## Read the datasets (Rany)

print("Generating Pseudopeople Data")
pseudo_df =  pseudo.generate_decennial_census(seed = 42)
df = spark.createDataFrame(pseudo_df)

## Sorting

df = df.repartition(6, col("last_name"))\
    .sortWithinPartitions(df.columns)

#spark_df.orderBy(spark_df.columns)

print(df.show(5))

## Deduplication



## Blocking

## Graph generation

## Connected Components

## Writing Output
