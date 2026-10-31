import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Windows environment bypass settings (Purani lines)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["HADOOP_USER_NAME"] = "hadoop"

# ---- ISS CLUSTER CODE SE REPLACE KAREIN (Dynamic Java Detection) ----
# Yeh command system registry se direct automatic binary rasta check karegi
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-11.0.32.101-hotspot"

os.environ["PATH"] = os.environ.get("JAVA_HOME", "") + "\\bin;" + os.environ.get("PATH", "")

# ==========================================
# 🚀 STEP 2: SPARK SESSION INITIALIZATION
# ==========================================
spark = SparkSession.builder \
    .appName("RealTimeFraudDetector Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*50)
print("⚡ Spark Session successfully start ho gayi hai...")
print("="*50 + "\n")

# ==========================================
# 📊 STEP 3: TRANSACTION SCHEMA BINDING
# ==========================================
schema = StructType([
    StructField("step", IntegerType()), StructField("type", StringType()),
    StructField("amount", DoubleType()), StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()), StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()), StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()), StructField("isFraud", IntegerType())
])

# ==========================================
# 📬 STEP 4: KAFKA MICRO-BATCH PULL LAYER
# ==========================================
# Read standard continuous pipeline configuration parameters
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize byte format parameters to matrix structure
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# ==========================================
# 🧠 STEP 5: REAL-TIME IN-MEMORY ANOMALY EVAL
# ==========================================
scored_df = parsed_df.withColumn(
    "prediction",
    when(
        (col("amount") > 200000) & 
        ((col("type") == "TRANSFER") | (col("type") == "CASH_OUT")) & 
        (col("oldbalanceOrg") < col("amount")), 
        1
    ).otherwise(0)
)

# ==========================================
# 📺 STEP 6: CONTINUOUS SCREEN MONITORING LOOP
# ==========================================
# Total bypass framework without forcing check-ins metadata folders
print("🚀 Real-Time scoring engine successfully active! Polling queue data channels...\n")

try:
    while True:
        # Check current processing telemetry context parameters refresh index
        # Reloading dataset elements live directly into console terminal view matrix
        current_data = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "financial-transactions") \
            .option("startingOffsets", "earliest") \
            .load()
            
        current_parsed = current_data.selectExpr("CAST(value AS STRING) as json_payload") \
            .select(from_json(col("json_payload"), schema).alias("data")) \
            .select("data.*")
            
        current_scored = current_parsed.withColumn(
            "prediction",
            when(
                (col("amount") > 200000) & 
                ((col("type") == "TRANSFER") | (col("type") == "CASH_OUT")) & 
                (col("oldbalanceOrg") < col("amount")), 1
            ).otherwise(0)
        )
        
        # Display latest telemetry entries directly without writeStream exceptions
        # PySpark 3.2.0 compatible empty dataframe check matrix
        if current_scored.first() is not None:

            current_scored.select("step", "type", "amount", "nameOrig", "prediction").show(10, truncate=False)
        else:
            print("⏳ Awaiting new transaction stream matrix inputs...")
            
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\n🛑 Real-Time continuous metrics tracking loop stopped smoothly.")
