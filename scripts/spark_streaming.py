import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Windows system variable bypass settings
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["HADOOP_USER_NAME"] = "hadoop"

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# 2. Spark Session initialization with advanced memory allocation configurations
spark = SparkSession.builder \
    .appName("RealTimeFraudDetector Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true -XX:+UseG1GC") \
    .getOrCreate()



# Logging silent logic to suppress redundant warnings
spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*50)
print("⚡ Spark Session successfully start ho gayi hai...")
print("="*50 + "\n")

# Transaction mapping schema data fields definition
schema = StructType([
    StructField("step", IntegerType()), StructField("type", StringType()),
    StructField("amount", DoubleType()), StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()), StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()), StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()), StructField("isFraud", IntegerType())
])

# Stream consumption core from local Kafka queue channel
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Serialized conversion layer mappings
parsed_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# Core Feature Decision Matrix for real-time anomaly tracking
scored_stream_df = parsed_stream_df.withColumn(
    "prediction",
    when(
        (col("amount") > 200000) & 
        ((col("type") == "TRANSFER") | (col("type") == "CASH_OUT")) & 
        (col("oldbalanceOrg") < col("amount")), 
        1
    ).otherwise(0)
)

# Filtering dynamic framework dataset to only grab anomalous rows
fraud_alerts_df = scored_stream_df

# Start the ingestion execution channel using strict 500ms micro-batch trigger logic
print("🚀 Real-Time scoring engine successfully active! Routing execution window...\n")

query = fraud_alerts_df.writeStream \
    .format("memory") \
    .queryName("fraud_alerts_table") \
    .outputMode("append") \
    .trigger(processingTime='500 milliseconds') \
    .start()


# Inline continuous execution loops monitoring structure
try:
    while True:
        # Pull transactional log indexes continuously from active system memory state
        spark.sql("SELECT * FROM fraud_alerts_table").show(20, truncate=False)
        time.sleep(1)
except KeyboardInterrupt:
    print("\n🛑 Streaming processing stopped smoothly.")