
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, try_cast # Import try_cast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os


spark = SparkSession.builder \
    .appName("BigBasketAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created successfully.")


dataset_path = r"C:\Users\Riad\Desktop\BigBasket Products.csv"

print(f"Attempting to load data from: {dataset_path}")

# Check if the path exists before trying to load
if not os.path.exists(dataset_path):
    print(f"Error: Dataset path not found at {dataset_path}")
    spark.stop()
    exit()


schema = StructType([
    StructField("index", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("sale_price", StringType(), True),
    StructField("market_price", StringType(), True),
    StructField("type", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("description", StringType(), True)
])

try:
    df = spark.read.csv(dataset_path, header=True, schema=schema)
    print("Data loaded successfully from CSV using manual schema.")

except Exception as e:
    print(f"Error loading data from {dataset_path}: {e}")
    spark.stop()
    exit()

# --- Data Cleaning and Preparation ---

print("\nStarting data cleaning and type casting...")

# Start with the loaded DataFrame
df_processed = df


# This replaces the standard .cast() calls that were failing.
df_processed = df_processed.withColumn("sale_price", try_cast(col("sale_price"), DoubleType()))
df_processed = df_processed.withColumn("market_price", try_cast(col("market_price"), DoubleType()))
df_processed = df_processed.withColumn("rating", try_cast(col("rating"), DoubleType()))

print("Attempted data types casting using try_cast for sale_price, market_price, and rating.")


print(f"Original number of rows: {df_processed.count()}") 
df_processed = df_processed.dropna(subset=['sale_price', 'market_price', 'rating'])

print(f"Number of rows after dropping rows with missing/failed price or rating casts: {df_processed.count()}")



print("\nDataFrame Schema after cleaning and type casting:")
df_processed.printSchema()

print("\nFirst 5 rows after cleaning:")
df_processed.show(5)

print("\nSummary Statistics after cleaning:")
df_processed.describe().show()

print("\nData Cleaning and Preparation completed.")



spark.stop()
print("\nSpark session stopped.")