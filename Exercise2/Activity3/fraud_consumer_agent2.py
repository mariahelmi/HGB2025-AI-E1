#This agent uses a sliding window (simulated) to perform velocity checks and score the transaction
import json
from collections import deque
import time
import base64
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = "localhost:9094"
TOPIC = "dbserver1.public.transactions"

# Kafka Consumer Setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='fraud-detection-group'
)

# Simulated In-Memory State
user_history = {}

def decode_decimal(encoded_bytes):
    """Decode Debezium's binary decimal format"""
    if not encoded_bytes:
        return 0.0
    try:
        decoded = base64.b64decode(encoded_bytes)
        value = int.from_bytes(decoded, byteorder='big', signed=True)
        return value / 100.0
    except Exception as e:
        print(f"Error decoding amount: {e}")
        return 0.0

def analyze_fraud(transaction):
    user_id = transaction['user_id']
    # FIXED: Decode the amount
    amount = decode_decimal(transaction['amount'])
    
    # Velocity Check
    now = time.time()
    if user_id not in user_history:
        user_history[user_id] = deque()
    
    user_history[user_id].append(now)
    while user_history[user_id] and user_history[user_id][0] < now - 60:
        user_history[user_id].popleft()
    
    velocity = len(user_history[user_id])
    
    # Fraud Scoring
    score = 0
    if velocity > 5:
        score += 40
    if amount > 4000:
        score += 50
    
    return score, amount

print("🔍 Fraud Detection Agent started...")

for message in consumer:
    payload = message.value.get('payload', {})
    data = payload.get('after')
    
    if data:
        fraud_score, amount = analyze_fraud(data)
        if fraud_score > 70:
            print(f"⚠️ HIGH FRAUD ALERT: User {data['user_id']} | Score: {fraud_score} | Amt: ${amount:.2f}")
        else:
            print(f"✅ Transaction OK: {data['id']} (Score: {fraud_score}, Amt: ${amount:.2f})")