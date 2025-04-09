import json
import sqlite3
from kafka import KafkaConsumer
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO
)

class AirQualityDB:
    def __init__(self, db_name='air_quality.db'):
        self.conn = sqlite3.connect(db_name)
        self._create_table()
        
    def _create_table(self):
        """Create table schema if not exists"""
        try:
            self.conn.execute('''CREATE TABLE IF NOT EXISTS measurements
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 timestamp DATETIME NOT NULL,
                 co REAL,
                 nox REAL,
                 no2 REAL,
                 temperature REAL,
                 humidity REAL)''')
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Database error: {e}")
            raise

    def store_measurement(self, data):
        """Insert processed data into database"""
        try:
            cur = self.conn.cursor()
            cur.execute('''INSERT INTO measurements
                (timestamp, co, nox, no2, temperature, humidity)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (
                    data['timestamp'],
                    data['sensor_data'].get('co'),
                    data['sensor_data'].get('nox'),
                    data['sensor_data'].get('no2'),
                    data['sensor_data'].get('temperature'),
                    data['sensor_data'].get('humidity')
                ))
            self.conn.commit()
            logging.info(f"Stored measurement: {data['timestamp']}")
        except sqlite3.Error as e:
            logging.error(f"Database insert failed: {e}")
            self.conn.rollback()

def main():
    # Initialize database handler
    db = AirQualityDB()
    
    # Configure Kafka consumer
    consumer = KafkaConsumer(
        'air-quality',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    logging.info("Starting air quality consumer")
    try:
        for message in consumer:
            try:
                data = message.value
                # Validate message structure
                if not all(k in data for k in ['timestamp', 'sensor_data']):
                    raise ValueError("Invalid message format")
                    
                # Process data
                processed = {
                    'timestamp': datetime.fromisoformat(data['timestamp']),
                    'sensor_data': {
                        k: float(v) if v is not None else None 
                        for k, v in data['sensor_data'].items()
                    }
                }
                
                # Store in database
                db.store_measurement(processed)
                
                # Optional: Add real-time alerting here
                if processed['sensor_data'].get('co') > 50:
                    logging.warning(f"High CO detected: {processed['sensor_data']['co']}")
                    
            except json.JSONDecodeError:
                logging.error("Failed to decode JSON message")
            except ValueError as e:
                logging.error(f"Data validation error: {e}")
            except Exception as e:
                logging.error(f"Unexpected error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Shutting down consumer")
    finally:
        consumer.close()
        db.conn.close()

if __name__ == "__main__":
    main()