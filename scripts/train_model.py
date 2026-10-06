import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix
import joblib

# 1. Folders ke paths set karein
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, 'data', 'paysim_dataset.csv')
MODEL_SAVE_PATH = os.path.join(BASE_DIR, 'models', 'fraud_model.pkl')
ENCODER_SAVE_PATH = os.path.join(BASE_DIR, 'models', 'label_encoder.pkl')

print("⏳ Step 1: PaySim Dataset se training data load ho raha hai...")
# Dataset 60 lakh rows ka hai, local system crash na ho isliye hum pehle 3,000,000 rows par train karenge
df = pd.read_csv(CSV_FILE_PATH, nrows=300000)

print("⚙️ Step 2: Data Preprocessing shuru ho rahi hai...")
# Transaction types (CASH_OUT, TRANSFER etc.) string format mein hain, unhe numbers mein badlein
le = LabelEncoder()
df['type'] = le.fit_transform(df['type'])

# Kal jab Spark real-time stream chalayega, toh uske liye label encoder save kar lete hain
os.makedirs(os.path.dirname(ENCODER_SAVE_PATH), exist_ok=True)
joblib.dump(le, ENCODER_SAVE_PATH)

# Model ko sikhane ke liye zaroori columns (Features) select karein
features = ['step', 'type', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest']
X = df[features]
y = df['isFraud'] # Yeh hamara actual answer key hai (0 = Safe, 1 = Fraud)

print("🧠 Step 3: Isolation Forest Model train ho raha hai (Dimaag ban raha hai)...")
# contamination = 0.002 matlab hum model ko bata rahe hain ki data mein lagbhag 0.2% anomalies (fraud) ho sakti hain
model = IsolationForest(n_estimators=100, contamination=0.002, random_state=42, n_jobs=-1)
model.fit(X)
print("✅ Model Training poori ho gayi hai!")

print("📊 Step 4: Model ka Performance test kar rahe hain...")
predictions = model.predict(X)
# Isolation Forest anomaly ko -1 aur normal ko 1 bolta hai. Ise hamare database format (1 aur 0) mein badlein:
predictions = np.where(predictions == -1, 1, 0)

print("\n--- Confusion Matrix ---")
print(confusion_matrix(y, predictions))

print("\n--- Classification Report ---")
print(classification_report(y, predictions))

print(f"💾 Step 5: Trained model ko serialize karke save kar rahe hain...")
joblib.dump(model, MODEL_SAVE_PATH)
print(f"✨ Safalta-purvak save ho gaya: '{MODEL_SAVE_PATH}'")