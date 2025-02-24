from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator # Evaluates model performance.

# Initialize Spark Session
spark = SparkSession.builder.appName("Game_Pricing_Classification").getOrCreate()

# Set log level to WARN to reduce unnecessary logs
spark.sparkContext.setLogLevel("WARN")

# Load dataset from HDFS
input_path = "hdfs://localhost:9000/steam_data/cleaned_feature_engineered_unified.csv/"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Feature Engineering: Add a binary column to indicate if a game is free
df = df.withColumn("is_free", when(col("price") == 0, 1).otherwise(0))  # 1 for Free, 0 for Paid

# Handle Null Values: Replace missing values with defaults for numeric columns
df = df.fillna({"average_playtime": 0, "positive_ratings": 0, "negative_ratings": 0, "sentiment_score": 0}) #Replaces NaN (missing values) with 0 for selected numeric columns.

# Encode Genres as Numeric: Use StringIndexer to handle categorical genres
indexer = StringIndexer(inputCol="genres", outputCol="genre_index", handleInvalid="skip")
df = indexer.fit(df).transform(df)

# Assemble Features: Combine relevant columns into a single feature vector
feature_cols = ["genre_index", "average_playtime", "positive_ratings", "negative_ratings", "sentiment_score"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
df = assembler.transform(df)

# Split Dataset: Train-Test split with 80%-20% ratio
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
train_df.cache() #cache() stores data in memory for faster access.
test_df.cache()

# Train Models
# Logistic Regression
lr = LogisticRegression(featuresCol="features", labelCol="is_free")
lr_model = lr.fit(train_df)

# Random Forest with adjusted maxBins for high cardinality
rf = RandomForestClassifier(featuresCol="features", labelCol="is_free", numTrees=10, maxBins=1554)
rf_model = rf.fit(train_df)

# Gradient Boosting with adjusted maxBins for high cardinality 
gbt = GBTClassifier(featuresCol="features", labelCol="is_free", maxIter=10, maxBins=1554) #maxIter=10 → Runs for 10 iterations.
gbt_model = gbt.fit(train_df)

# Predictions: Apply each model to the test dataset to generate predictions.
lr_preds = lr_model.transform(test_df)
rf_preds = rf_model.transform(test_df)
gbt_preds = gbt_model.transform(test_df)

# Evaluation Metrics: Accuracy, F1 Score, and AUROC
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="is_free", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="is_free", metricName="f1")
evaluator_auroc = BinaryClassificationEvaluator(labelCol="is_free", metricName="areaUnderROC")

# Logistic Regression Metrics
print("\nLogistic Regression Metrics:")
print("  Accuracy:", evaluator_accuracy.evaluate(lr_preds))
print("  F1 Score:", evaluator_f1.evaluate(lr_preds))
print("  AUROC:", evaluator_auroc.evaluate(lr_preds))

# Random Forest Metrics
print("\nRandom Forest Metrics:")
print("  Accuracy:", evaluator_accuracy.evaluate(rf_preds))
print("  F1 Score:", evaluator_f1.evaluate(rf_preds))
print("  AUROC:", evaluator_auroc.evaluate(rf_preds))

# Gradient Boosting Metrics
print("\nGradient Boosting Metrics:")
print("  Accuracy:", evaluator_accuracy.evaluate(gbt_preds))
print("  F1 Score:", evaluator_f1.evaluate(gbt_preds))
print("  AUROC:", evaluator_auroc.evaluate(gbt_preds))

# Sample Predictions for each model with game name
print("\nSample Predictions for Logistic Regression:")
lr_preds.select("name", "features", "is_free", "prediction", "probability").show(20, truncate=False)

print("\nSample Predictions for Random Forest:")
rf_preds.select("name", "features", "is_free", "prediction", "probability").show(20, truncate=False)

print("\nSample Predictions for Gradient Boosting:")
gbt_preds.select("name", "features", "is_free", "prediction", "probability").show(20, truncate=False)

# Stop Spark Session
spark.stop()
