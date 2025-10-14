from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
data = spark.readStream.format("socket") \
.option("host", "localhost").option("port", 9999) \
.load()

# Parse JSON data into columns using the defined schema
parsed_data = data.select(from_json(col("value").cast("string"), schema) \
.alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
parsed_data_with_timestamp = parsed_data.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Compute aggregations: total fare and average distance grouped by driver_id

# Define a function to write each batch to a CSV file

    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    

# Use foreachBatch to apply the function to each micro-batch

query.awaitTermination()
