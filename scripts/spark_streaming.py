import os
# Windows environment bypass setting
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
# Yeh command Windows ke native permission check ko bypass kar degi
os.environ["HADOOP_USER_NAME"] = "hadoop"

import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 1. Paths configuration
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
MODEL_PATH = os.path.join(BASE_DIR, 'models', 'fraud_model.pkl')
ENCODER_PATH = os.path.join(BASE_DIR, 'models', 'label_encoder.pkl')

# 2. Spark Session initialize karein (Kafka aur Cassandra connectors ke sath)
spark = SparkSession.builder \
    .appName("RealTimeFraudDetector Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") \
    .getOrCreate()

print("⚡ Spark Session successfully start ho gayi hai...")

# 3. Micro-batches ke liye JSON schema define karein
schema = StructType([
    StructField("step", IntegerType()), StructField("type", StringType()),
    StructField("amount", DoubleType()), StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()), StructField("newbalanceOrig", StringType()),
    StructField("nameDest", StringType()), StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()), StructField("isFraud", IntegerType())
])

# 4. Kafka topic se live stream consume karein
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "financial-transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Raw binary data ko readable text aur columns mein badlein
parsed_stream_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# 5. ML Model aur Encoder ko load karein
model = joblib.load(MODEL_PATH)
le = joblib.load(ENCODER_PATH)

# Python 3.11+ Serialization ke liye direct partition logic use karenge (UDF error fix)
def process_partition(iterator):
    import joblib
    import pandas as pd
    
    # Model ko workers ke andar locally re-load karein taaki serialization crash na ho
    local_model = joblib.load(MODEL_PATH)
    local_le = joblib.load(ENCODER_PATH)
    
    for row in iterator:
        try:
            type_encoded = local_le.transform([row['type']])[0]
        except:
            type_encoded = 0
            
        features = [[
            row['step'], type_encoded, row['amount'], 
            row['oldbalanceOrg'], row['newbalanceOrig'], 
            row['oldbalanceDest'], row['newbalanceDest']
        ]]
        
        pred = local_model.predict(features)
        prediction_val = 1 if pred == -1 else 0
        
        # Sirf tabhi yield karein jab transaction fraud ho
        if prediction_val == 1:
            yield (
                row['nameOrig'], row['step'], row['type'], float(row['amount']),
                row['nameOrig'], float(row['oldbalanceOrg']), float(row['newbalanceOrig']),
                row['nameDest'], 1, 1
            )

# 6. Stream ko dynamic structure mein pass karein
# Isse dynamic model mapping hogi bina serialisation failure ke
fraud_alerts_df = parsed_stream_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: \
        batch_df.rdd.mapPartitions(process_partition).toDF([
            "transaction_id", "step", "type", "amount", 
            "nameOrig", "oldbalanceOrg", "newbalanceOrig", 
            "nameDest", "isFraud", "prediction"
        ]).write \
          .format("org.apache.spark.sql.cassandra") \
          .option("keyspace", "fraud_detection") \
          .option("table", "flagged_transactions") \
          .mode("append") \
          .save() if not batch_df.isEmpty() else None
    ) \
    .option("checkpointLocation", os.path.join(BASE_DIR, 'checkpoint')) \
    .start()

print("🚀 Real-Time Pipeline Framework Active ho chuka hai! Awaiting stream data...")
fraud_alerts_df.awaitTermination()