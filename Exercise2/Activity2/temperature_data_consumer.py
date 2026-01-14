import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
import psycopg
from psycopg import sql

DB_NAME = "office_db"
DB_USER = "postgres"
DB_PASSWORD = "postgrespw"
DB_HOST = "localhost"
DB_PORT = 5432


# -------------------------
# Periodically compute average over last 10 minutes
# -------------------------
try:
    while True:
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        ## Fetch the data from the choosen source (to be implemented)
        
        conn = psycopg.connect(
            f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"
        )

        with conn.cursor() as cur:
            cur.execute("""
                SELECT AVG(temperature) as avg_temp
                FROM temperature_readings
                WHERE recorded_at >= %s 
            """, (ten_minutes_ago,)) ## Querying the data so it should give me the average of the last 10 minutes
            
            result = cur.fetchone()
            avg_temp = result[0] if result and result[0] is not None else None
        
        conn.close()

        if avg_temp is not None:
            print(f"{datetime.now()} - Average temperature last 10 minutes: {avg_temp:.2f} °C")
        else:
            print(f"{datetime.now()} - No data in last 10 minutes.")
        time.sleep(600)  # every 10 minutes
except KeyboardInterrupt:
    print("Stopped consuming data.")
finally:
    print("Exiting.")
