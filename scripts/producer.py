import time
import json
import os
import pandas as pd
from kafka import KafkaProducer

# 1. Kafka Producer setup (Localhost:9092)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'financial-transactions'

# 2. Path verification (Check kar rahe hain ki data data folder mein hai)
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), '../data/paysim_dataset.csv')

print("⏳ PaySim Dataset read ho raha hai...")

if not os.path.exists(CSV_FILE_PATH):
    print(f"❌ Error: Dataset file '{CSV_FILE_PATH}' nahi mili!")
    print("Kripya check karein ki aapne 'data' folder mein file ka naam 'paysim_dataset.csv' hi rakha hai.")
    exit()

# RAM crash na ho, isliye chunks mein read karenge
chunks = pd.read_csv(CSV_FILE_PATH, chunksize=1000)

print(f"🚀 Live Streaming shuru ho rahi hai topic: '{TOPIC_NAME}' par...")

try:
    for chunk in chunks:
        for index, row in chunk.iterrows():
            # CSV ke data ko JSON structure mein badalna
            transaction_data = {
                'step': int(row['step']),
                'type': str(row['type']),
                'amount': float(row['amount']),
                'nameOrig': str(row['nameOrig']),
                'oldbalanceOrg': float(row['oldbalanceOrg']),
                'newbalanceOrig': float(row['newbalanceOrig']),
                'nameDest': str(row['nameDest']),
                'oldbalanceDest': float(row['oldbalanceDest']),
                'newbalanceDest': float(row['newbalanceDest']),
                'isFraud': int(row['isFraud'])
            }
            
            # Kafka Topic mein data bhejna
            producer.send(TOPIC_NAME, transaction_data)
            
            # 0.1 second ka delay (real-time stream feel ke liye)
            time.sleep(0.1)
            
        print("✨ 1000 transactions safalta-purvak bhej diye gaye hain...")

except KeyboardInterrupt:
    print("\n🛑 Streaming ko user dwara rok diya gaya hai.")
finally:
    producer.close()
