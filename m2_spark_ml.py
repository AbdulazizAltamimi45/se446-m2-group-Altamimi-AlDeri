# ============================================
# SE446 - Milestone 2: Spark ML Pipeline
# Tasks 5-7: Mohamad Al Deri (ID: 230151)
# ============================================
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

def main():
    output_lines = []

    def log(msg=""):
        print(msg)
        sys.stdout.flush()
        output_lines.append(msg)

    log("="*50)
    log("Starting Phase B: Spark ML Pipeline")
    log("Author: Mohamad Al Deri (ID: 230151)")
    log("="*50)

    # Initialize Spark Session
    spark = SparkSession.builder.appName("M2_PhaseB_ML_Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load Data from HDFS
    log("Loading data from HDFS...")
    raw_df = spark.read.csv("hdfs:///data/chicago_crimes.csv", header=True, inferSchema=True)

    # ============================================
    # Task 5: Feature Engineering Pipeline
    # ============================================
    log("\n[Task 5] Applying 5% sampling and feature engineering...")
    df_sampled = raw_df.sample(fraction=0.05, seed=42)
    df_ml = df_sampled

    if "label" not in df_ml.columns:
        df_ml = df_ml.withColumn("label", col("Arrest").cast("integer"))
    if "PrimaryType" not in df_ml.columns:
        df_ml = df_ml.withColumn("PrimaryType", col("Primary Type"))
    if "Domestic_str" not in df_ml.columns:
        df_ml = df_ml.withColumn("Domestic_str", col("Domestic").cast("string"))
    if "Date" in df_ml.columns and "Hour" not in df_ml.columns:
        df_ml = df_ml.withColumn("Hour", hour(to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a")))

    df_ml = df_ml.dropna(subset=["District", "PrimaryType", "Hour", "Domestic_str", "label"])

    type_indexer     = StringIndexer(inputCol="PrimaryType",  outputCol="crime_index",    handleInvalid="skip")
    domestic_indexer = StringIndexer(inputCol="Domestic_str", outputCol="domestic_index", handleInvalid="skip")
    assembler        = VectorAssembler(inputCols=["District", "crime_index", "Hour", "domestic_index"], outputCol="features")

    train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    log(f"Training set size: {train_df.count()} rows")
    log(f"Testing  set size: {test_df.count()} rows")

    # ============================================
    # Task 6: Train and Evaluate Three Models
    # ============================================
    log("\n[Task 6] Training Logistic Regression, Random Forest, and GBT...")
    lr  = LogisticRegression(maxIter=100, regParam=0.01, featuresCol="features", labelCol="label")
    rf  = RandomForestClassifier(numTrees=100, maxDepth=5, featuresCol="features", labelCol="label", seed=42)
    gbt = GBTClassifier(maxIter=50, maxDepth=5, featuresCol="features", labelCol="label", seed=42)

    models   = {"Logistic Regression": lr, "Random Forest": rf, "GBT": gbt}
    results  = []
    rf_model     = None
    best_pipeline = None

    bin_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    mc_eval  = MulticlassClassificationEvaluator(labelCol="label")

    for name, classifier in models.items():
        log(f"  Training {name}...")
        pipeline = Pipeline(stages=[type_indexer, domestic_indexer, assembler, classifier])

        start_time = time.time()
        model = pipeline.fit(train_df)
        train_time = time.time() - start_time

        if name == "Random Forest":
            rf_model      = model.stages[-1]
            best_pipeline = model

        predictions = model.transform(test_df)
        auc  = bin_eval.evaluate(predictions)
        acc  = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
        f1   = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
        prec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
        rec  = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})

        results.append((name, round(auc,4), round(acc,4), round(f1,4), round(prec,4), round(rec,4), round(train_time,2)))

    log("\n" + "="*80)
    log(f"{'Model':<20} | {'AUC-ROC':<8} | {'Accuracy':<8} | {'F1 Score':<8} | {'Precision':<9} | {'Recall':<8} | {'Time (s)':<8}")
    log("-"*80)
    for r in results:
        log(f"{r[0]:<20} | {r[1]:<8} | {r[2]:<8} | {r[3]:<8} | {r[4]:<9} | {r[5]:<8} | {r[6]:<8}")
    log("="*80)

    # ============================================
    # Task 7: Feature Importances & Interpretation
    # ============================================
    log("\n[Task 7] Random Forest Feature Importances...")
    importances   = rf_model.featureImportances.toArray()
    feature_names = ["District", "crime_index", "Hour", "domestic_index"]
    imp_sorted    = sorted(zip(feature_names, importances), key=lambda x: -x[1])
    for fname, imp in imp_sorted:
        bar = "#" * int(imp * 40)
        log(f"  {fname:<18} {imp:.4f}  {bar}")

    # Save best model
    log("\nSaving the best model (Random Forest) to HDFS...")
    save_path = "hdfs:///user/aoaltamimi/best_rf_model"
    best_pipeline.write().overwrite().save(save_path)
    log(f"Model successfully saved to {save_path}")

    log("\nPipeline execution completed successfully.")

    # Write all output to HDFS so it survives log aggregation issues
    out_rdd = spark.sparkContext.parallelize(output_lines, 1)
    out_rdd.saveAsTextFile("hdfs:///user/aoaltamimi/task11_output")
    print("Output written to HDFS: hdfs:///user/aoaltamimi/task11_output")

    spark.stop()

if __name__ == "__main__":
    main()
