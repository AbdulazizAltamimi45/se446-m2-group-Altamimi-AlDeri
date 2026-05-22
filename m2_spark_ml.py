# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Tasks 5-7: Mohamad Al Deri (ID: 230151)
# ============================================
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

def main():
    print("="*50)
    print("Starting Phase B: Spark ML Pipeline")
    print("Author: Mohamad Al Deri (ID: 230151)")
    print("="*50)

    # Initialize Spark Session
    spark = SparkSession.builder.appName("M2_PhaseB_ML_Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load Data from HDFS
    print("Loading data from HDFS...")
    raw_df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)




    # ============================================
    # Task 5: Feature Engineering Pipeline
    # ============================================
    print("\n[Task 5] Applying 5% sampling and feature engineering...")
    df_sampled = raw_df.sample(fraction=0.05, seed=42)

    df_ml = df_sampled

    # Create label if it doesn't exist
    if "label" not in df_ml.columns:
        df_ml = df_ml.withColumn("label", col("Arrest").cast("integer"))

    # Standardize Primary Type naming
    if "PrimaryType" not in df_ml.columns:
        df_ml = df_ml.withColumn("PrimaryType", col("Primary Type"))

    # Handle Domestic status string conversion
    if "Domestic_str" not in df_ml.columns:
        df_ml = df_ml.withColumn("Domestic_str", col("Domestic").cast("string"))

    # Extract Hour from Date ONLY if Date exists and Hour doesn't (Cluster mode)
    if "Date" in df_ml.columns and "Hour" not in df_ml.columns:
        df_ml = df_ml.withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))

    # Drop any rows with nulls in our target features to prevent MLlib crashes
    df_ml = df_ml.dropna(subset=["District", "PrimaryType", "Hour", "Domestic_str", "label"])

    type_indexer = StringIndexer(inputCol="PrimaryType", outputCol="crime_index", handleInvalid="skip")
    domestic_indexer = StringIndexer(inputCol="Domestic_str", outputCol="domestic_index", handleInvalid="skip")
    assembler = VectorAssembler(inputCols=["District", "crime_index", "Hour", "domestic_index"], outputCol="features")

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    print(f"Training set size: {train_df.count()} rows")





    # ============================================
    # Task 6: Train and Evaluate Three Models
    # ============================================
    print("\n[Task 6] Training Logistic Regression, Random Forest, and GBT...")
    lr = LogisticRegression(maxIter=100, regParam=0.01, featuresCol="features", labelCol="label")
    rf = RandomForestClassifier(numTrees=100, maxDepth=5, featuresCol="features", labelCol="label", seed=42)
    gbt = GBTClassifier(maxIter=50, maxDepth=5, featuresCol="features", labelCol="label", seed=42)

    models = {"Logistic Regression": lr, "Random Forest": rf, "GBT": gbt}
    results = []
    
    bin_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    mc_eval = MulticlassClassificationEvaluator(labelCol="label")
    rf_model = None

    for name, classifier in models.items():
        pipeline = Pipeline(stages=[type_indexer, domestic_indexer, assembler, classifier])
        
        start_time = time.time()
        model = pipeline.fit(train_df)
        train_time = time.time() - start_time
        
        if name == "Random Forest":
            rf_model = model.stages[-1]
            best_pipeline = model  # <--- Add this line
        
        predictions = model.transform(test_df)
        auc = bin_eval.evaluate(predictions)
        acc = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
        f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
        prec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
        rec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})
        
        results.append((name, round(auc,4), round(acc,4), round(f1,4), round(prec,4), round(rec,4), round(train_time,2)))

    # Print Results Table
    print("\n" + "="*80)
    print(f"{'Model':<20} | {'AUC-ROC':<8} | {'Accuracy':<8} | {'F1 Score':<8} | {'Precision':<9} | {'Recall':<8} | {'Time (s)':<8}")
    print("-" * 80)
    for r in results:
        print(f"{r[0]:<20} | {r[1]:<8} | {r[2]:<8} | {r[3]:<8} | {r[4]:<9} | {r[5]:<8} | {r[6]:<8}")
    print("="*80)




    # ============================================
    # Task 7: Feature Importances & Interpretation
    # ============================================
    print("\n[Task 7] Random Forest Feature Importances...")
    importances = rf_model.featureImportances.toArray()
    feature_names = ["District", "crime_index", "Hour", "domestic_index"]
    
    # Sort and print
    imp_sorted = sorted(zip(feature_names, importances), key=lambda x: -x[1])
    for name, imp in imp_sorted:
        bar = "#" * int(imp * 40)
        print(f"  {name:<18} {imp:.4f}  {bar}")


    print("\nSaving the best model (Random Forest) to HDFS...")
    best_model = trained_models["Random Forest"]
    
    save_path = "hdfs:///user/230151/project/m2/best_model"
    best_model.write().overwrite().save(save_path)
    print(f"Model successfully saved to {save_path}")

    print("\nPipeline execution completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()