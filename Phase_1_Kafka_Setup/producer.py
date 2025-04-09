import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import json
import time
import logging

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

def parse_custom_datetime(date_str, time_str):
    """Parse DD/MM/YYYY and HH.MM.SS formats to datetime"""
    try:
        # Explicitly convert to strings and handle float representations
        date_str = str(date_str)
        time_str = str(time_str).replace('.', ':', 2)  # Replace first two '.' with ':'
        
        return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    except ValueError as e:
        logging.error(f"Failed to parse datetime: {date_str} {time_str} - {e}")
        return None

# Load and preprocess dataset
try:
    # Read CSV with explicit dtype for Date/Time columns
    df = pd.read_csv(
        'AirQualityUCI.csv',
        delimiter=';',
        decimal=',',
        dtype={'Date': str, 'Time': str}  # Force string type
    )
    logging.info("Successfully loaded dataset")
    
    # Handle missing values marked with -200
    df.replace(-200, None, inplace=True)
    
    # Ensure columns are strings (redundant but safe)
    df['Date'] = df['Date'].astype(str)
    df['Time'] = df['Time'].astype(str)
    
    # Parse datetime
    df['datetime'] = df.apply(
        lambda row: parse_custom_datetime(row['Date'], row['Time']), 
        axis=1
    )
    
    # Drop rows with invalid timestamps
    df = df.dropna(subset=['datetime']).sort_values('datetime')
    
except Exception as e:
    logging.error(f"Data loading failed: {e}")
    raise

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks='all'
)

# Stream records with time synchronization
prev_time = None
for index, row in df.iterrows():
    try:
        record = row.to_dict()
        current_time = record['datetime']
        
        # Calculate relative delay (1 hour = 1 second)
        if prev_time is not None:
            time_diff = (current_time - prev_time).total_seconds()
            delay = time_diff / 3600  # Compress 1 hour to 1 second
            time.sleep(delay)
            
        # Prepare message payload
        payload = {
            'timestamp': current_time.isoformat(),
            'sensor_data': {
                'co': record['CO(GT)'],
                'nox': record['NOx(GT)'],
                'no2': record['NO2(GT)'],
                'temperature': record['T'],
                'humidity': record['RH']
            }
        }
        
        # Send to Kafka
        producer.send('air-quality', value=payload)
        logging.info(f"Sent record: {payload['timestamp']}")
        prev_time = current_time
        
    except Exception as e:
        logging.error(f"Failed to send record {index}: {e}")
        continue

producer.flush()