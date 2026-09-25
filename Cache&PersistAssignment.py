from pyspark.sql import SparkSession
from pyspark import StorageLevel
import time
 ## Creating a Spark session
spark = SparkSession.builder \
    .appName("Spark Cache and Persist Practice") \
    .getOrCreate()
## STEP 1 
data = [
    {"id": 1, "name": "Alice", "age": 28},
    {"id": 2, "name": "Bob", "age": 33},
    {"id": 3, "name": "Cathy", "age": 45},
    {"id": 4, "name": "David", "age": 23},
    {"id": 5, "name": "Eva", "age": 31}
]

df = spark.createDataFrame(data)

print("SAMPLE DATAFRAME:")
df.show()

print("SCHEMA:")
df.printSchema()

## STEP 2: Creating a Larger DataFrame
large_df = spark.range(1_000_000)

print("LARGE DATAFRAME:")
large_df.show(5)


## STEP 3 Cache

cache_df = large_df.cache()

# First action
start = time.time()
cache_count1 = cache_df.count()
cache_time1 = time.time() - start

# Second action
start = time.time()
cache_count2 = cache_df.count()
cache_time2 = time.time() - start

print("\nCACHE RESULTS:")
print("Count:", cache_count1)
print(f"First execution time: {cache_time1:.4f} seconds")
print(f"Second execution time: {cache_time2:.4f} seconds")
print("Storage Level:", cache_df.storageLevel)

# Clean up cache before persist experiment
cache_df.unpersist()



## STEP 4 Persist
persist_df = large_df.persist(StorageLevel.MEMORY_AND_DISK)

# First action
start = time.time()
persist_count1 = persist_df.count()
persist_time1 = time.time() - start

# Second action
start = time.time()
persist_count2 = persist_df.count()
persist_time2 = time.time() - start

print("\nPERSIST RESULTS:")
print("Count:", persist_count1)
print(f"First execution time: {persist_time1:.4f} seconds")
print(f"Second execution time: {persist_time2:.4f} seconds")
print("Storage Level:", persist_df.storageLevel)


## STEP 5: COMPARE RESULTS

print("\nCOMPARISON:")

print(f"Cache first execution:  {cache_time1:.4f} seconds")
print(f"Cache second execution: {cache_time2:.4f} seconds")

print(f"Persist first execution:  {persist_time1:.4f} seconds")
print(f"Persist second execution: {persist_time2:.4f} seconds")

print("\nCache Storage Level:")
print(cache_df.storageLevel)

print("\nPersist Storage Level:")
print(persist_df.storageLevel)

## STEP 6 Clean-up
persist_df.unpersist()

spark.stop()
