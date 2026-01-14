# This agent calculates a running average for each user and flags transactions that are significantly higher than their usual behavior (e.g., $3\sigma$ outliers).

import json
import statistics
import base64
import struct
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
    group_id='anomaly-detection-group'
)

# In-memory store for user spending patterns
user_spending_profiles = {}

def decode_decimal(encoded_bytes):
    """Decode Debezium's binary decimal format"""
    if not encoded_bytes:
        return 0.0
    try:
        # Decode base64
        decoded = base64.b64decode(encoded_bytes)
        # Convert bytes to integer (big-endian)
        value = int.from_bytes(decoded, byteorder='big', signed=True)
        # Debezium uses scale=2, so divide by 100
        return value / 100.0
    except Exception as e:
        print(f"Error decoding amount: {e}")
        return 0.0

def analyze_pattern(data):
    user_id = data['user_id']
    # FIXED: Decode the amount from bytes
    amount = decode_decimal(data['amount'])
    
    if user_id not in user_spending_profiles:
        user_spending_profiles[user_id] = []
    
    history = user_spending_profiles[user_id]
    
    # Analyze if transaction is an outlier
    is_anomaly = False
    if len(history) >= 3:
        avg = statistics.mean(history)
        stdev = statistics.stdev(history) if len(history) > 1 else 0
        
        # If amount is > 3x the average
        if amount > (avg * 3) and amount > 500:
            is_anomaly = True
    
    # Update profile
    history.append(amount)
    if len(history) > 50:
        history.pop(0)
    
    return is_anomaly, amount

print("🧬 Anomaly Detection Agent started...")

for message in consumer:
    payload = message.value.get('payload', {})
    data = payload.get('after')
    
    if data:
        is_fraudulent_pattern, amount = analyze_pattern(data)
        
        if is_fraudulent_pattern:
            print(f"🚨 ANOMALY DETECTED: User {data['user_id']} spent ${amount:.2f} (Significantly higher than average)")
        else:
            print(f"📊 Profile updated for User {data['user_id']} - Amount: ${amount:.2f}")