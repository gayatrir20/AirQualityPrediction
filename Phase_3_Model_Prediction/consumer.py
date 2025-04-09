import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
import logging
from datetime import datetime
import joblib
import os

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

# Load the trained model and feature columns list
try:
    model = joblib.load('air_quality_prediction_model.pkl')
    feature_columns = pd.read_csv('feature_columns.csv').iloc[:, 0].tolist()
    logging.info(f"Successfully loaded model with {len(feature_columns)} features")
except Exception as e:
    logging.error(f"Failed to load model: {e}")
    raise

# Initialize CSV file for storing predictions
predictions_file = 'predictions.csv'
if not os.path.isfile(predictions_file):
    with open(predictions_file, mode='w') as f:
        f.write('timestamp,actual_value,predicted_value,error\n')

def main():
    # Configure Kafka consumer to consume from 'air-quality-test' topic
    consumer = KafkaConsumer(
        'air-quality-test',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=60000  # Stop after 60 seconds of inactivity
    )

    logging.info("Starting air quality prediction consumer")
    
    try:
        for message in consumer:
            try:
                # Parse incoming message from Kafka topic
                data = message.value
                
                timestamp = data['timestamp']
                actual_value = float(data['actual_value'])
                features = data['features']
                
                # Prepare feature vector for prediction =
                feature_vector = [features.get(col, np.nan) for col in feature_columns]
                
                # Handle missing values (mean imputation)
                feature_vector = [np.nanmean(feature_vector) if np.isnan(val) else val for val in feature_vector]
                
                # Make prediction using pre-trained model
                prediction = model.predict([feature_vector])[0]
                
                # Calculate error (absolute difference between actual and predicted values)
                error = abs(actual_value - prediction)
                
                # Store results in CSV file
                with open(predictions_file, mode='a') as f:
                    f.write(f"{timestamp},{actual_value},{prediction},{error}\n")
                
                logging.info(f"Prediction - Timestamp: {timestamp}, Actual: {actual_value}, Predicted: {prediction:.2f}, Error: {error:.2f}")
            
            except KeyError as e:
                logging.error(f"Missing key in message: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Shutting down consumer")
        
if __name__ == "__main__":
    main()
