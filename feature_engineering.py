from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, when, udf, explode #explode → Converts an array into multiple rows (used for genres).
from pyspark.ml.feature import MinMaxScaler # Normalizes numeric features to a range of [0,1].
from pyspark.ml.linalg import Vectors, VectorUDT #Handles vectorized numerical features for ML models.

# Initialize Spark session
spark = SparkSession.builder.appName("Feature Engineering").getOrCreate()

# Load dataset
input_path = "hdfs://localhost:9000/steam_data/cleaned_steam.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# 1. Binary One-Hot Encoding for Genres (Dynamic Extraction)
# Extract unique genres from the dataset
unique_genres = (
    df.select(explode(split(col("genres"), ";")).alias("genre"))
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

# Add binary columns for each genre
for genre in unique_genres:
    df = df.withColumn(f'is_{genre.replace(" ", "_")}', when(col("genres").contains(genre), 1).otherwise(0))

# 2. Pricing Strategy
# Add price tiers
df = df.withColumn(
    'price_tier',
    when(col('price') == 0, 'Free')
    .when((col('price') > 0) & (col('price') <= 10), 'Low')
    .when((col('price') > 10) & (col('price') <= 30), 'Medium')
    .otherwise('High')
)

# Normalize price . Converts "price" into a vector column (price_vec), required for MinMaxScaler.
to_vector = udf(lambda x: Vectors.dense([float(x)]) if x is not None else Vectors.dense([0.0]), VectorUDT())
df = df.withColumn("price_vec", to_vector("price"))
scaler = MinMaxScaler(inputCol="price_vec", outputCol="price_scaled")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df).drop("price_vec") #Drops intermediate column price_vec.

# 3. User Reviews Sentiment
df = df.withColumn("total_ratings", col("positive_ratings") + col("negative_ratings"))
df = df.withColumn(
    "sentiment_score",
    when(col("total_ratings") > 0, col("positive_ratings") / col("total_ratings")).otherwise(None) #If total ratings are zero, assigns None.
)
df = df.withColumn("has_achievements", when(col("achievements") > 0, 1).otherwise(0))

# 4. Platform Compatibility
#df = df.withColumn("num_platforms", size(split(col("platforms"), ";")))

# 5. Success Metric (Improved)
# Define a mapping function to approximate owners range as numeric values
def owners_to_numeric(owners):
    if owners is None:
        return 0
    try:
        ranges = owners.split("-") # Owners are stored as a range (e.g., "100,000-200,000")
        return (int(ranges[0].replace(",", "")) + int(ranges[1].replace(",", ""))) // 2 #This function extracts the lower and upper bounds, converts them into integers, and calculates the average.
    except Exception:
        return 0

owners_to_numeric_udf = udf(owners_to_numeric) #Registers the function owners_to_numeric() as a UDF (User-Defined Function).

df = df.withColumn("owners_numeric", owners_to_numeric_udf(col("owners")))

# Calculate success metric
df = df.withColumn(
    "success_metric",
    (col("positive_ratings") * 0.5 + col("average_playtime") * 0.3 + col("owners_numeric") * 0.2)
)

# Drop complex fields not supported in CSV . Identifies columns with complex data types (VectorUDT), which are not supported in CSV format
unsupported_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (VectorUDT))]
df = df.drop(*unsupported_columns)

# Save the engineered dataset in CSV format 
output_path = "hdfs://localhost:9000/steam_data/feature_engineered_steam.csv"
df.write.option("header", "true").csv(output_path, mode="overwrite")

print(f"Feature engineering completed. Data saved to: {output_path}")
spark.stop()