from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, when

# Initialize Spark session
spark = SparkSession.builder.appName("Data Cleaning for steam.csv").master('local[*]').getOrCreate() # Runs Spark locally using all CPU cores

# Input and output paths
input_path = "hdfs://localhost:9000/steam_data/steam.csv"
output_path = "hdfs://localhost:9000/steam_data/cleaned_steam.csv"

try:
    print(f"Processing file: steam.csv")

    # Load the steam.csv file with inferred schema
    df = spark.read.csv(input_path, header=True, inferSchema=True) #header=True → Uses the first row as column names. inferSchema=True → Automatically detects column data types.

    # Standardize column names: renaming "appid" or "steam_appid" to "app_id"
    if "steam_appid" in df.columns:
        df = df.withColumnRenamed("steam_appid", "app_id")
    if "appid" in df.columns:
        df = df.withColumnRenamed("appid", "app_id")

    # Data cleaning steps:
    # 1. Removing duplicates
    cleaned_df = df.dropDuplicates()

    # 2. Trimming whitespace from all string columns
    cleaned_df = cleaned_df.select([trim(col(c)).alias(c) if c != "price" else col(c) for c in cleaned_df.columns])

    # 3. Treat empty price as null but keep numeric and zero prices
    if 'price' in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "price",
            when(trim(col("price")) == "", None).otherwise(trim(col("price")).cast("float")) # Empty prices should not be treated as "0" but as missing data.
        )

    # 4. Convert numeric columns to appropriate types
    numeric_columns = ["positive_ratings", "negative_ratings", "average_playtime", "median_playtime", "price"]
    for col_name in numeric_columns:
        if col_name in cleaned_df.columns:
            cleaned_df = cleaned_df.withColumn(col_name, col(col_name).cast("float"))

    # 5. Removing rows with all null values
    cleaned_df = cleaned_df.dropna(how='all')

    # 6. Removing rows with null values in critical columns
    critical_columns = [col for col in ['app_id', 'name'] if col in cleaned_df.columns]
    cleaned_df = cleaned_df.dropna(subset=critical_columns)

    # Save cleaned data in CSV format with a header
    cleaned_df.write.option("header", "true").csv(output_path, mode="overwrite")  #Includes column names (header=True). Overwrites any existing file at output_path.
    print(f"Saved cleaned file to: {output_path}")

except Exception as e:
    import logging
    logging.error(f"Error processing file steam.csv: {e}")

print("Data cleaning for steam.csv completed successfully.")
spark.stop()