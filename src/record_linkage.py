import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
import pseudopeople as pseudo

print("Loading Spark")
spark = SparkSession.builder.appName("Record Linkage").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()

## Read the datasets (Rany)

print("Generating Pseudopeople Data")

df_1 = spark.read.csv("/home/nachiket/RLA_CL_EXTRACT/data/pse/pse_sample4.1.1", header=True, sep='\t')
df_2 = spark.read.csv("/home/nachiket/RLA_CL_EXTRACT/data/pse/pse_sample4.1.2", header=True, sep='\t')

#df_1.show(5)

## Sorting

# df = df.repartition(6, col("last_name"))\
#     .sortWithinPartitions(df.columns)

#df_1 = df_1.sort(cols=['last_name', 'first_name', 'middle_initial','age', 'date_of_birth', 'street_number', 'street_name', 'unit_number', 'city', 'state', 'zipcode', 'housing_type', 'relationship_to_reference_person', 'sex', 'race_ethnicity', 'year'])
df_1.sort(col("last_name"),col("first_name")).show(5)
df_1 = df_1.show(5)

## Deduplication


'''
    Adding index to the dataframe
'''
#df = df.withColumn('index', monotonically_increasing_id())




## Blocking

## Graph generation

## Connected Components

## Writing Output
