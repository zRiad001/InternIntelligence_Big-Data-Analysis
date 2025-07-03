from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("YourAnalysisApp") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Session created successfully.")

spark.sparkContext.setLogLevel("WARN")

dataset_path = r"C:\Users\Riad\Desktop\BigBasket Products.csv"

print(f"Attempting to load data from: {dataset_path}")

if not os.path.exists(dataset_path):
    print(f"Error: Dataset path not found at {dataset_path}")
    spark.stop()
    exit()

try:
    df = spark.read.csv(dataset_path, header=True, inferSchema=True)

    print("\nStarting data cleaning and type casting...")

    df_cleaned = df

    print(f"Original number of rows: {df_cleaned.count()}")
    df_cleaned = df_cleaned.dropna(subset=['sale_price', 'market_price', 'rating'])
    print(f"Number of rows after dropping rows with missing prices or rating: {df_cleaned.count()}")

    from pyspark.sql.functions import col

    df_cleaned = df_cleaned.withColumn("sale_price", col("sale_price").cast("double"))
    df_cleaned = df_cleaned.withColumn("market_price", col("market_price").cast("double"))
    df_cleaned = df_cleaned.withColumn("rating", col("rating").cast("double"))

    print("Data types cast for sale_price, market_price, and rating.")

    print("\nDataFrame Schema after cleaning and type casting:")
    df_cleaned.printSchema()

    print("\nFirst 5 rows after cleaning:")
    df_cleaned.show(5)

    print("\nSummary Statistics after cleaning (should now show stats for numeric columns):")
    df_cleaned.describe().show()

    df_processed = df_cleaned

    print("\nData Cleaning and Preparation completed.")

    print("Data loaded successfully from CSV.")

except Exception as e:
    print(f"Error loading data from {dataset_path}: {e}")
    spark.stop()
    exit()

print("\nDataFrame Schema:")
df.printSchema()

print("\nFirst 5 rows:")
df.show(5)

print(f"\nTotal number of records: {df.count()}")

print("\nSummary Statistics (for numerical columns):")
df.describe().show()

print(f"\nNumber of columns: {len(df.columns)}")

spark.stop()
print("\nSpark session stopped.")
