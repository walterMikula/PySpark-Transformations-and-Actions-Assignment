from pyspark.sql import SparkSession


# STEP 1
# Create SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Partitioning and Bucketing Practice") \
    .enableHiveSupport() \
    .getOrCreate()


# STEP 2
# Create product sales data
data = [
    (101, "Laptop", "Electronics", 1200, "2024-01-15"),
    (102, "Headphones", "Electronics", 200, "2024-01-17"),
    (103, "Coffee Maker", "Home", 85, "2024-01-20"),
    (104, "Desk", "Office", 300, "2024-02-01"),
    (105, "Monitor", "Electronics", 400, "2024-02-03"),
    (106, "Blender", "Home", 60, "2024-02-10"),
    (107, "Chair", "Office", 150, "2024-02-14"),
    (108, "Keyboard", "Electronics", 90, "2024-02-18"),
    (109, "Lamp", "Home", 40, "2024-02-20"),
    (110, "Notebook", "Office", 10, "2024-02-21")
]

columns = [
    "product_id",
    "product_name",
    "category",
    "price",
    "sale_date"
]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Display DataFrame
print("\nPRODUCT DATA:")
df.show()


# STEP 3
# Write the DataFrame as Parquet partitioned by category
df.write \
    .mode("overwrite") \
    .partitionBy("category") \
    .parquet("products_partitioned")

print("\nPartitioned Parquet data written to products_partitioned")


# STEP 4
# Remove the table if it already exists
# This allows the program to be run multiple times
spark.sql("DROP TABLE IF EXISTS products_bucketed")

# Create a bucketed Spark table
df.write \
    .bucketBy(5, "price") \
    .sortBy("price") \
    .mode("overwrite") \
    .saveAsTable("products_bucketed")

print("\nBUCKETED TABLE:")
spark.sql("""
    SELECT *
    FROM products_bucketed
""").show()


# STEP 5
# Read the partitioned Parquet data
partitioned_df = spark.read.parquet("products_partitioned")

# Register it as a temporary view so we can query it using SQL
partitioned_df.createOrReplaceTempView("products_partitioned_view")


# Query the partitioned data
print("\nPARTITIONED DATA QUERY:")

spark.sql("""
    SELECT *
    FROM products_partitioned_view
    WHERE category = 'Electronics'
      AND price > 100
""").show()


# Run the equivalent query against the bucketed table
print("\nBUCKETED DATA QUERY:")

spark.sql("""
    SELECT *
    FROM products_bucketed
    WHERE category = 'Electronics'
      AND price > 100
""").show()


# Explanation
print("""
PARTITIONING VS BUCKETING Explanation:

Partitioning organizes the data into separate directories based on
the values of a column. The data is partitioned by category, creating 
separate partitions for Electronics, Home, and Office.

Because the query filters category = 'Electronics', Spark can use
partition pruning and avoid reading the Home and Office partitions.

Bucketing divides records into a fixed number of buckets using the
values of a specified column. The data is divided into 5 buckets 
based on price and sorted by price within the buckets.

Partitioning is useful when queries frequently filter on a partition
column. Bucketing can help organize data for operations such as joins
and aggregations involving the bucket column.
""")


spark.stop()