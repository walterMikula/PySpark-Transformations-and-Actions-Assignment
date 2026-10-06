from pyspark.sql import SparkSession
from pyspark.sql.functions import col # changed to col because of python built in round f
from pyspark.sql.types import *
import random
import time
from datetime import datetime, timedelta


# STEP 1: SETUP AND DATA PREPARATION

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SharedVariablesAssignment") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()


# Sample data generation
def generate_transaction_data(num_records=10000):
    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
        "Beauty"
    ]

    regions = [
        "North",
        "South",
        "East",
        "West",
        "Central"
    ]

    data = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_records):
        transaction_id = f"TXN_{i:06d}"
        customer_id = f"CUST_{random.randint(1, 2000):05d}"
        product_category = random.choice(categories)
        region = random.choice(regions)
        amount = round(random.uniform(10.0, 500.0), 2)
        quantity = random.randint(1, 5)
        transaction_date = base_date + timedelta(
            days=random.randint(0, 365)
        )

        data.append((
            transaction_id,
            customer_id,
            product_category,
            region,
            amount,
            quantity,
            transaction_date
        ))

    return data


# Create the dataset
transaction_data = generate_transaction_data()


# Define schema
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", DateType(), True)
])


# Create DataFrame
df = spark.createDataFrame(transaction_data, schema)

# Cache DataFrame
df.cache()


print("\nSAMPLE TRANSACTION DATA:")
df.show(10)

print(f"Total transactions: {df.count()}")


# STEP 2: BROADCAST VARIABLES


# Reference data
category_discounts = {
    "Electronics": 0.10,
    "Clothing": 0.15,
    "Books": 0.05,
    "Home": 0.08,
    "Sports": 0.12,
    "Beauty": 0.20
}


region_tax_rates = {
    "North": 0.08,
    "South": 0.06,
    "East": 0.09,
    "West": 0.07,
    "Central": 0.05
}


# Create broadcast variables
broadcast_discounts = spark.sparkContext.broadcast(
    category_discounts
)

broadcast_tax_rates = spark.sparkContext.broadcast(
    region_tax_rates
)


print("\nBROADCAST VARIABLES CREATED")

print(
    "Category Discounts:",
    broadcast_discounts.value
)

print(
    "Region Tax Rates:",
    broadcast_tax_rates.value
)


# APPROACH 1: BROADCAST VARIABLES WITH MAP()

start_time = time.time()


broadcast_result = df.rdd.map(
    lambda row: (
        row.transaction_id,
        row.customer_id,
        row.product_category,
        row.region,
        row.amount,

        broadcast_discounts.value.get(
            row.product_category,
            0.0
        ),

        broadcast_tax_rates.value.get(
            row.region,
            0.0
        ),

        row.amount
        * (
            1 - broadcast_discounts.value.get(
                row.product_category,
                0.0
            )
        )
        * (
            1 + broadcast_tax_rates.value.get(
                row.region,
                0.0
            )
        )
    )
)


# Force Spark to execute transformation
broadcast_count = broadcast_result.count()

broadcast_time = time.time() - start_time


print("\nBROADCAST MAP RESULTS:")

print(
    f"Records processed: {broadcast_count}"
)

print(
    f"Broadcast map execution time: "
    f"{broadcast_time:.4f} seconds"
)


# APPROACH 2: DATAFRAME JOINS

# Convert discount dictionary into DataFrame
discount_df = spark.createDataFrame(
    [
        (category, rate)
        for category, rate
        in category_discounts.items()
    ],
    [
        "category",
        "discount_rate"
    ]
)


# Convert tax dictionary into DataFrame
tax_df = spark.createDataFrame(
    [
        (region, rate)
        for region, rate
        in region_tax_rates.items()
    ],
    [
        "tax_region",
        "tax_rate"
    ]
)


# Start timing join approach
start_time = time.time()


# Join transaction data with discount data
joined_df = df.join(
    discount_df,
    df.product_category == discount_df.category,
    "left"
)


# Join with tax data
joined_df = joined_df.join(
    tax_df,
    df.region == tax_df.tax_region,
    "left"
)


# Calculate final price
result_df = joined_df.withColumn(
    "final_price",
    col("amount")
    * (1 - col("discount_rate"))
    * (1 + col("tax_rate"))
)


# Force Spark to execute
join_count = result_df.count()

join_time = time.time() - start_time


print("\nDATAFRAME JOIN RESULTS:")


result_df.select(
    "transaction_id",
    "product_category",
    "region",
    "amount",
    "discount_rate",
    "tax_rate",
    "final_price"
).show(10)


print(
    f"Records processed: {join_count}"
)

print(
    f"DataFrame join execution time: "
    f"{join_time:.4f} seconds"
)


# PERFORMANCE COMPARISON

print("\nPERFORMANCE COMPARISON:")

print(
    f"Broadcast map time: "
    f"{broadcast_time:.4f} seconds"
)

print(
    f"DataFrame join time: "
    f"{join_time:.4f} seconds"
)


# STEP 3: ACCUMULATORS

# Accumulator for total transactions
total_transactions_acc = \
    spark.sparkContext.accumulator(0)


# Accumulator for high-value transactions
high_value_transactions_acc = \
    spark.sparkContext.accumulator(0)


# Accumulator for total quantity
total_quantity_acc = \
    spark.sparkContext.accumulator(0)


# Accumulator for total sales
total_sales_acc = \
    spark.sparkContext.accumulator(0.0)


# PROCESS TRANSACTIONS

def process_transaction(row):

    # Count each transaction
    total_transactions_acc.add(1)

    # Add quantity sold
    total_quantity_acc.add(
        row.quantity
    )

    # Add transaction amount
    total_sales_acc.add(
        row.amount
    )

    # Count high-value transactions
    if row.amount > 400:
        high_value_transactions_acc.add(1)


# Apply function to every row
df.foreach(process_transaction)


# PRINT ACCUMULATOR RESULTS

print("\nACCUMULATOR RESULTS:")

print(
    f"Total transactions processed: "
    f"{total_transactions_acc.value}"
)

print(
    f"High-value transactions "
    f"(amount > $400): "
    f"{high_value_transactions_acc.value}"
)

print(
    f"Total quantity sold: "
    f"{total_quantity_acc.value}"
)

print(
    f"Total sales amount: "
    f"${total_sales_acc.value:.2f}"
)


spark.stop()