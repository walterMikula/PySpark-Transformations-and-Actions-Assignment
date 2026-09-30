from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc, when, col


# Create SparkSession
spark = SparkSession.builder \
    .appName("DataFrames Practice") \
    .getOrCreate()


# STEP 1: LOAD AND EXPLORE

# Load CSV into a DataFrame
df = spark.read.csv(
    "people.csv",
    header=True,
    inferSchema=True
)

print("\n PEOPLE DATA ")
df.show()

print("\n SCHEMA ")
df.printSchema()

# Count total records
total_records = df.count()

print("\n TOTAL RECORDS ")
print("Total records:", total_records)


# STEP 2: FILTERING AND SELECTING

# Filter for people who live in New York
ny_df = df.filter(col("city") == "New York")

print("\n NEW YORK RECORDS ")
ny_df.show()


# Select only name and salary
ny_name_salary = ny_df.select("name", "salary")

print("\n NEW YORK NAME AND SALARY ")
ny_name_salary.show()


# Count New York records
ny_count = ny_df.count()

print("\n NEW YORK RECORD COUNT ")
print("New York records:", ny_count)


# Calculate average salary for New York
print("\n NEW YORK AVERAGE SALARY ")
ny_df.agg(avg("salary").alias("average_salary")).show()



# STEP 3: GROUPING AND AGGREGATION

# Group by city and calculate average salary
city_avg_salary = df.groupBy("city") \
    .agg(avg("salary").alias("average_salary")) \
    .orderBy(desc("average_salary"))

print("\n AVERAGE SALARY BY CITY ")
city_avg_salary.show()


# STEP 4: DATAFRAME TRANSFORMATION

# Add income_bracket column based on salary
income_df = df.withColumn(
    "income_bracket",
    when(col("salary") < 80000, "Low")
    .when((col("salary") >= 80000) & (col("salary") < 100000), "Mid")
    .otherwise("High")
)

print("\n DATA WITH INCOME BRACKETS ")
income_df.show()


# Count records in each income bracket
bracket_counts = income_df.groupBy("income_bracket").count()

print("\n INCOME BRACKET COUNTS ")
bracket_counts.show()


# Stop SparkSession
spark.stop()