import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import json
import time
import logging
import numpy as np

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

def parse_custom_datetime(date_str, time_str):
    """Parse DD/MM/YYYY and HH.MM.SS formats to datetime"""
    try:
        # Ensure both date_str and time_str are strings
        date_str = str(date_str)
        time_str = str(time_str).replace('.', ':')  # Replace '.' with ':' in time string
        
        return datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    except ValueError as e:
        logging.error(f"Failed to parse datetime: {date_str} {time_str} - {e}")
        return None

# Load and preprocess dataset
try:
    # Load dataset
    df = pd.read_csv('AirQualityUCI.csv', delimiter=';', decimal=',', na_values=-200)
    logging.info("Successfully loaded dataset")
    
    # Drop unnecessary columns and handle missing values
    df.drop(['PT08.S1(CO)', 'PT08.S2(NMHC)', 'PT08.S3(NOx)', 
           'PT08.S4(NO2)', 'PT08.S5(O3)', 'Unnamed: 15', 'Unnamed: 16'], axis=1, inplace=True)
    df.dropna(inplace=True)
    
    # Ensure 'Date' and 'Time' columns are strings
    df['Date'] = df['Date'].astype(str)
    df['Time'] = df['Time'].astype(str)
    
    # Parse datetime
    df['datetime'] = df.apply(
        lambda row: parse_custom_datetime(row['Date'], row['Time']), 
        axis=1
    )
    
    # Drop rows with invalid timestamps
    df = df.dropna(subset=['datetime']).sort_values('datetime')
    
    # Feature engineering for lagged features (useful for prediction)
    target_column = 'CO(GT)'
    for lag in [1, 3, 6, 12, 24]:
        df[f'{target_column}_lag_{lag}'] = df[target_column].shift(lag)

    # Drop NaN values created by lag features
    df.dropna(inplace=True)
    
    # Split into train and test sets (chronological, 80/20 split)
    train_size = int(len(df) * 0.8)
    test_data = df.iloc[train_size:]
    
    logging.info(f"Test data shape: {test_data.shape}")
    
except Exception as e:
    logging.error(f"Data loading failed: {e}")
    raise

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    retries=5,
    acks='all'
)

# Stream test records with time synchronization
prev_time = None
for index, row in test_data.iterrows():
    try:
        record = row.to_dict()
        current_time = record['datetime']
        
        # Prepare message payload - including all features for prediction
        payload = {
            'timestamp': str(current_time),
            'actual_value': record[target_column],  # For evaluation purposes
            'features': {col: record[col] for col in record if col != 'datetime' and col != target_column}
        }
        
        # Send to Kafka topic 'air-quality-test'
        producer.send('air-quality-test', value=payload)
        logging.info(f"Sent test record: {payload['timestamp']}")
        
        prev_time = current_time
        
        # Simulate streaming delay (optional)
        time.sleep(0.1)  # Adjust delay as needed
        
    except Exception as e:
        logging.error(f"Failed to send record {index}: {e}")
        continue

producer.flush()
logging.info("Finished streaming test data")