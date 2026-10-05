from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Employee JSON Practice") \
    .getOrCreate()

# Read the multi-line JSON file
employees_df = spark.read \
    .option("multiline", "true")\
    .json("employees.json")

# Display the schema
print("\nEMPLOYEE SCHEMA:")
employees_df.printSchema()

# Display the original employee data
print("\nORIGINAL EMPLOYEE DATA:")
employees_df.show()

# Replace missing salary values with 40000
cleaned_df = employees_df.na.fill(40000, subset=["salary"])

# Display the cleaned data
print("\nCLEANED EMPLOYEE DATA:")
cleaned_df.show()

# Write the cleaned DataFrame to JSON
cleaned_df.write \
    .mode("overwrite") \
    .json("employees_cleaned.json")

print("\nCleaned employee data written to employees_cleaned.json")

# Stop Spark session
spark.stop()