from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Create SparkSession
spark = SparkSession.builder \
    .appName("TemperatureStreaming") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True)
])

# Read streaming JSON files
stream_df = spark.readStream \
    .schema(schema) \
    .json("temp_input")

# Select required columns and filter
filtered_df = stream_df \
    .select("device_id", "temperature") \
    .filter("temperature > 70")

# Write results to console
query = filtered_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Keep stream running
query.awaitTermination()