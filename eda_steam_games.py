from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, count, split, explode, when, desc

# Initialize SparkSession
spark = SparkSession.builder.appName("Steam Game EDA").getOrCreate()

# Load the feature-engineered CSV file from HDFS
input_path = "hdfs://localhost:9000/steam_data/cleaned_feature_engineered_unified.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Inspect schema and basic stats
print("Schema:")
df.printSchema()

# Count rows and columns
print(f"Total rows: {df.count()}, Total columns: {len(df.columns)}")

# Filter out rows with missing, null, or invalid genres
df_filtered = df.filter(col("genres").isNotNull() & (col("genres") != "") & (col("genres") != "0"))

# Genres' Impact 
# Split genres into individual entries and analyze
df_filtered = df_filtered.withColumn("genre_array", split(col("genres"), ";")) #Splits multiple genres (e.g., "Action;RPG") into a list.
exploded_df = df_filtered.withColumn("genre", explode(col("genre_array"))) #Expands each genre into separate rows using explode().

print("Top Genres by Success Metric:")
exploded_df.groupBy("genre").agg(mean("success_metric").alias("avg_success")) \
    .orderBy(desc("avg_success")).show(10, truncate=False)  #Computes the average success_metric per genre

print("Genres with the Highest Average Play Time:")
exploded_df.groupBy("genre").agg(mean("average_playtime").alias("avg_playtime")) \
    .orderBy(desc("avg_playtime")).show(10, truncate=False) #Computes average playtime per genre.

# Pricing Strategy 
print("Correlation between Price and Success Metric:")
price_success_corr = df.stat.corr("price", "success_metric")  #Computes the correlation coefficient between price and success_metric.
print(f"Correlation: {price_success_corr}")

print("Pricing Tiers and Success Metric:")  #Categorizes price into tiers (Free, Low, Medium, High).Computes average success metric per price tier.

df.withColumn("price_tier", when(col("price") == 0, "Free")
              .when(col("price") <= 10, "Low")
              .when((col("price") > 10) & (col("price") <= 30), "Medium")
              .otherwise("High")) \
    .groupBy("price_tier").agg(mean("success_metric").alias("avg_success"),
                               count("*").alias("game_count")) \
    .orderBy(desc("avg_success")).show(truncate=False)

# System Requirements 
# Check if the "pc_minimum" column exists before analysis
#if "pc_minimum" in df.columns:
 #   print("System Requirements and Success Metric:")
  #  df.groupBy("pc_minimum").agg(mean("success_metric").alias("avg_success")) \
   #     .orderBy(desc("avg_success")).show(5, truncate=False)
#
 #   print("Minimum Requirements by Play Time:")
  #  df.groupBy("pc_minimum").agg(mean("average_playtime").alias("avg_playtime")) \
   #     .orderBy(desc("avg_playtime")).show(5, truncate=False)

#  User Reviews and Sentiment
print("Sentiment Score vs Success Metric:") #Analyzes how sentiment score affects success.
df.groupBy("sentiment_score").agg(mean("success_metric").alias("avg_success")) \
    .orderBy(desc("avg_success")).show(10)

print("Positive Ratings vs Success Metric Correlation:")
positive_success_corr = df.stat.corr("positive_ratings", "success_metric")
print(f"Correlation: {positive_success_corr}")

# Price Range Analysis 
print("Price Range vs Average Playtime:")
df.withColumn("price_range", when(col("price") < 10, "<10")
              .when((col("price") >= 10) & (col("price") < 30), "10-30")
              .otherwise("30+")) \
    .groupBy("price_range").agg(mean("average_playtime").alias("avg_playtime")) \
    .orderBy(desc("avg_playtime")).show(truncate=False)
