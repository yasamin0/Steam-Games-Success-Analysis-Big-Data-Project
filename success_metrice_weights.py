from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors

# Initialize Spark session
spark = SparkSession.builder.appName("Success Metric Optimization").getOrCreate()

# Load feature-engineered dataset
input_path = "hdfs://localhost:9000/steam_data/feature_engineered_steam.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Select features and target variable
features = ["positive_ratings", "average_playtime", "owners_numeric"]
target = "success_metric"  # The column you initially defined as a success measure

# Remove null values from selected columns
df = df.dropna(subset=features + [target])

# Assemble feature columns into a single vector
assembler = VectorAssembler(inputCols=features, outputCol="features")
df = assembler.transform(df).select(col("features"), col(target).alias("label")) #Keeps only features and target (renamed as "label" for training).

# Train Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(df) #trains the model by finding the best weights for predicting success_metric.

# Extract feature coefficients (weights)
coefficients = lr_model.coefficients.toArray()

# Normalize coefficients to sum up to 1
normalized_weights = coefficients / coefficients.sum() 

# Print the computed weights
print("Optimized Success Metric Weights:")
for i, feature in enumerate(features):
    print(f"{feature}: {normalized_weights[i]:.4f}")

# Stop Spark session
spark.stop()