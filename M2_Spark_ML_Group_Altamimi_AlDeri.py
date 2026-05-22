#!/usr/bin/env python
# coding: utf-8

# In[2]:


# ============================================
# SE446 - Milestone 2: Spark Analytics (Phase A)
# Group: Altamimi-AlDeri
#
# Tasks 1, 2, 3, 4: Abdulaziz Altamimi (ID: 230714)
# ============================================
import os
import sys
import warnings
warnings.filterwarnings('ignore')

# Must be set before the JVM starts — fixes Windows "Python worker timeout" error
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["_JAVA_OPTIONS"] = "-Djava.net.preferIPv4Stack=true"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, when,
    round as spark_round
)

# Auto-detect environment: cluster vs local
ON_CLUSTER = (
    os.environ.get('YARN_CONF_DIR') is not None or
    os.environ.get('HADOOP_CONF_DIR') is not None or
    os.path.exists('/data/chicago_crimes.csv')
)

mode = 'CLUSTER' if ON_CLUSTER else 'LOCAL'
print(f'Running in {mode} mode')
print(f'Python executable: {sys.executable}')

if ON_CLUSTER:
    spark = (
        SparkSession.builder
        .appName('SE446-M2-PhaseA-Altamimi-AlDeri')
        .getOrCreate()
    )
else:
    spark = (
        SparkSession.builder
        .appName('SE446-M2-PhaseA-Altamimi-AlDeri')
        .master('local[*]')
        .config('spark.driver.memory', '2g')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.pyspark.python', sys.executable)
        .config('spark.pyspark.driver.python', sys.executable)
        .getOrCreate()
    )

spark.sparkContext.setLogLevel('WARN')
os.makedirs('output', exist_ok=True)
print(f'Master: {spark.sparkContext.master}')
print(f'Spark version: {spark.version}')
print(f'Web UI: {spark.sparkContext.uiWebUrl}')


# In[3]:


# ============================================
# Data Loading / Generation
# Author: Abdulaziz Altamimi (ID: 230714)
# ============================================

import random
import pandas as pd

if ON_CLUSTER:
    DATA_PATH = 'hdfs:///data/chicago_crimes.csv'
    print(f'Loading data from HDFS: {DATA_PATH}')
    df = spark.read.csv(DATA_PATH, header=True, inferSchema=True)
else:
    # ---- LOCAL MODE: Generate realistic sample data ----
    from pyspark.sql import Row
    import random
    random.seed(42)

    crime_profiles = {
        "NARCOTICS":         0.85,
        "PROSTITUTION":      0.80,
        "WEAPONS VIOLATION": 0.60,
        "BATTERY":           0.30,
        "ASSAULT":           0.25,
        "ROBBERY":           0.15,
        "THEFT":             0.10,
        "BURGLARY":          0.08,
        "MOTOR VEHICLE THEFT": 0.06,
        "CRIMINAL DAMAGE":   0.05,
    }
    districts = list(range(1, 26))

    LOCATIONS = [
        "STREET", "RESIDENCE", "APARTMENT", "SIDEWALK", "OTHER",
        "PARKING LOT / GARAGE(NON.RESID.)", "ALLEY",
        "SCHOOL, PUBLIC, BUILDING", "SMALL RETAIL STORE", "RESTAURANT"
    ]
    def generate_row():
        crime_type = random.choice(list(crime_profiles.keys()))
        base_rate = crime_profiles[crime_type]
        district = random.choice(districts)
        hour_val = random.randint(0, 23)
        domestic = random.random() < 0.15
        year = random.randint(2001, 2023)
        location=random.choice(LOCATIONS)
        arrest_prob = base_rate + (0.20 if domestic else 0)
        if 2 <= hour_val <= 5:
            arrest_prob -= 0.10
        arrest_prob = max(0.01, min(0.99, arrest_prob))
        arrest = random.random() < arrest_prob
        return Row(
            District=district, PrimaryType=crime_type,
            Hour=hour_val, Domestic_str=str(domestic).lower(),
            Arrest=arrest, label=int(arrest), Year=year, 
            LocationDescription=location
        )

    rows = [generate_row() for _ in range(10000)]
    df = spark.createDataFrame(rows)

    df = (df.withColumnRenamed('PrimaryType', 'Primary Type').withColumnRenamed('LocationDescription', 'Location Description'))

if ON_CLUSTER:
    total = df.count()
else:
    try:
        total = len(rows)
    except NameError:
        total = df.count()
print(f'Total records: {total:,}')
print('Schema:')
df.printSchema()
df.show(5, truncate=False)


# ---
# ## Phase A: Spark DataFrame Analytics
# 

# ---
# ### Task 1: Crime Type Distribution (Spark DataFrame)
# 
# **Goal**: Count crimes by `Primary Type`, show top 10 ordered by count descending.  
# **Author**: Abdulaziz Altamimi (ID: 230714)
# 

# In[4]:


# ============================================
# Task 1: Crime Type Distribution
# Author: Abdulaziz Altamimi (ID: 230714)
# ============================================
print('TASK 1: Crime Type Distribution (Spark DataFrame)')
print()
print('Top 10 Crime Types by Count:')

crime_dist = (
    df.groupBy("Primary Type")
      .count()
      .orderBy(col('count').desc())
).show(10, truncate=False)


# ---
# ### Task 2: Location Hotspots (Spark SQL)
# 
# **Goal**: Find the top 10 crime location descriptions using `spark.sql()` — not the DataFrame API.  
# **Author**: Abdulaziz Altamimi (ID: 230714)

# In[5]:


# ============================================
# Task 2: Location Hotspots (Spark SQL)
# Author: Abdulaziz Altamimi (ID: 230714)
# ============================================
print('TASK 2: Location Hotspots (Spark SQL)')
print()
print('Top 10 Crime Location Hotspots:')

# Register DataFrame as a SQL temp view
df.createOrReplaceTempView("crimes")
hotspots = spark.sql("""
    SELECT `Location Description`, COUNT(*) as total
    FROM crimes
    GROUP BY `Location Description`
    ORDER BY total DESC
    LIMIT 10
""").show()


# ---
# ### Task 3: Crime Trend Over Years (DataFrame + Visualization)
# 
# **Goal**: Show crime count per year; include a matplotlib line chart in local mode.  
# **Author**: Abdulaziz Altamimi (ID: 230714)
# 

# In[6]:


# ============================================
# Task 3: Crime Trend Over Years
# Author: Abdulaziz Altamimi (ID: 230714)
# ============================================

try:
    import matplotlib
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

print('TASK 3: Crime Trend Over Years')

yearly = df.groupBy('Year').count().orderBy('Year').toPandas()
if ON_CLUSTER and HAS_MPL:
    matplotlib.use('Agg')
print()
print('Crime Count Per Year:')
print(yearly.to_string(index=False))

if HAS_MPL:
    try:
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.plot(yearly['Year'], yearly['count'],
                marker='o', color='steelblue', linewidth=2, markersize=5)
        ax.fill_between(yearly['Year'], yearly['count'], alpha=0.15, color='steelblue')
        ax.set_title('Chicago Crimes per Year', fontsize=14, fontweight='bold')
        ax.set_xlabel('Year')
        ax.set_ylabel('Number of Crimes')
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        out_path = '/tmp/task3_crime_trend.png' if ON_CLUSTER else 'output/task3_crime_trend.png'
        plt.savefig(out_path, dpi=120, bbox_inches='tight')
        plt.show()
        print(f'Chart saved to: {out_path}')
    except Exception as e:
        print(f'Visualization skipped: {e}')
else:
    print('Visualization skipped: matplotlib not available')


# ---
# ### Task 4: Arrest Rate Analysis (DataFrame)
# 
# **Goal**: Compute (a) overall arrest rate and (b) arrest rate broken down by crime type.  
# **Author**: Abdulaziz Altamimi (ID: 230714)
# 

# In[ ]:


# ============================================
# Task 4: Arrest Rate Analysis
# Author: Abdulaziz Altamimi (ID: 230714)
# ============================================
print('TASK 4: Arrest Rate Analysis')

# Cast Arrest boolean to integer (0/1)
df = df.withColumn('label', col('Arrest').cast('integer'))

# Overall arrest rate
overall = df.agg(
    count('*').alias('total_crimes'),
    count(when(col('label') == 1, True)).alias('total_arrests'),
    (avg('label')).alias('arrest_rate_pct')
).toPandas()

total_crimes = int(overall['total_crimes'][0])
total_arrests = int(overall['total_arrests'][0])
arrest_rate_pct = float(overall['arrest_rate_pct'][0])

print()
print('Overall Statistics:')
print(f'Total Crimes: {total_crimes:,}')
print(f'Total Arrests: {total_arrests:,}')
print(f'Arrest Rate: {arrest_rate_pct:.2f}')

print()
print('Crime Types with Highest Arrest Rates:')

# Arrest rate per crime type
per_type = (
    df.groupBy("Primary Type")
      .agg(
          count('*').alias('total'),
          count(when(col('label') == 1, True)).alias('arrests')
      )
      .withColumn(
          'arrest_rate_pct',
          spark_round((col('arrests') / col('total')), 2)
      )
      .orderBy(col('arrest_rate_pct').desc())
).show()


# ---
# ### Task 5: Feature Engineering Pipeline
# 
# Author: Mohamad Al Deri (ID: 230151)

# In[ ]:


# ============================================
# Task 5: Feature Engineering Pipeline
# Author: Mohamad Al Deri (ID: 230151)
# ============================================
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql.functions import col, hour, to_timestamp

# 1. Apply 5%
df_sampled = df.sample(fraction=0.05, seed=42)

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

# 3. Define Transformers
type_indexer = StringIndexer(inputCol="PrimaryType", outputCol="crime_index", handleInvalid="skip")
domestic_indexer = StringIndexer(inputCol="Domestic_str", outputCol="domestic_index", handleInvalid="skip")
assembler = VectorAssembler(inputCols=["District", "crime_index", "Hour", "domestic_index"], outputCol="features")

# 4. Train/Test Split
train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
train_df.cache()

print(f"Sampled Training set size: {train_df.count()} rows")
print(f"Sampled Testing set size: {test_df.count()} rows")


# ---
# ### Task 6: Train and Evaluate Three Models
# Author: Mohamad Al Deri (ID: 230151)
# 

# In[ ]:


# ============================================
# Task 6: Train and Evaluate Three Models
# Author: Mohamad Al Deri (ID: 230151)
# ============================================
import time
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

# 1. Define Models
lr = LogisticRegression(maxIter=100, regParam=0.01, featuresCol="features", labelCol="label")
rf = RandomForestClassifier(numTrees=100, maxDepth=5, featuresCol="features", labelCol="label", seed=42)
gbt = GBTClassifier(maxIter=50, maxDepth=5, featuresCol="features", labelCol="label", seed=42)

models = {"Logistic Regression": lr, "Random Forest": rf, "GBT": gbt}
results = []
trained_models = {}

# 2. Setup Evaluators
bin_eval = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
mc_eval = MulticlassClassificationEvaluator(labelCol="label")

# 3. Train and Evaluate Loop
for name, classifier in models.items():
    print(f"Training {name}...")
    pipeline = Pipeline(stages=[type_indexer, domestic_indexer, assembler, classifier])

    start_time = time.time()
    model = pipeline.fit(train_df)
    train_time = time.time() - start_time
    trained_models[name] = model

    predictions = model.transform(test_df)

    auc = bin_eval.evaluate(predictions)
    acc = mc_eval.evaluate(predictions, {mc_eval.metricName: "accuracy"})
    f1 = mc_eval.evaluate(predictions, {mc_eval.metricName: "f1"})
    prec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedPrecision"})
    rec = mc_eval.evaluate(predictions, {mc_eval.metricName: "weightedRecall"})

    tp = predictions.filter("label = 1 AND prediction = 1").count()
    tn = predictions.filter("label = 0 AND prediction = 0").count()
    fp = predictions.filter("label = 0 AND prediction = 1").count()
    fn = predictions.filter("label = 1 AND prediction = 0").count()

    results.append({
        "Model": name, "AUC-ROC": round(auc, 4), "Accuracy": round(acc, 4),
        "F1 Score": round(f1, 4), "Precision": round(prec, 4), "Recall": round(rec, 4),
        "TP": tp, "TN": tn, "FP": fp, "FN": fn, "Time (s)": round(train_time, 2)
    })

# 4. Display Table
results_df = pd.DataFrame(results)
print("\n--- Model Comparison ---")
print(results_df.to_string(index=False))


# ---
# ### Task 7: Feature Importances & Interpretation
#  Author: Mohamad Al Deri (ID: 230151)

# In[ ]:


# ============================================
# Task 7: Feature Importances & Interpretation
# Author: Mohamad Al Deri (ID: 230151)
# ============================================

# 1. Extract importances from the trained Random Forest model
rf_model = trained_models["Random Forest"].stages[-1]
importances = rf_model.featureImportances.toArray()
feature_names = ["District", "crime_index", "Hour", "domestic_index"]

imp_df = pd.DataFrame({"Feature": feature_names, "Importance": importances}).sort_values(by="Importance", ascending=False)

print("--- Random Forest Feature Importances ---")
print(imp_df.to_string(index=False))

# 2. Visualize
if HAS_MPL:
    plt.figure(figsize=(8, 4))
    plt.bar(imp_df["Feature"], imp_df["Importance"], color='#2ca02c')
    plt.title("Random Forest Feature Importances")
    plt.ylabel("Importance Score")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()


# ---

# In[ ]:


# Save the best model (Random Forest Pipeline)
best_model = trained_models["Random Forest"]

# Save locally since you are on Colab
import shutil
shutil.rmtree("best_rf_model", ignore_errors=True)
best_model.write().overwrite().save("best_rf_model")
print("Model successfully saved!")


# In[ ]:


# Stop Spark session when done
spark.stop()
print('SparkSession stopped.')

