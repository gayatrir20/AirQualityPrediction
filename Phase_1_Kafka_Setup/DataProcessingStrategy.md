**Data Preprocessing Plan for Streaming Data to Kafka in Producer Script:**
Loading Dataset: The dataset is loaded using `pandas.read_csv()` with some parameters.
Handling Missing Values: Missing values are represented by `-200` and replaced by `None` to keep missing value representation consistent.
Parsing and Creating a Datetime Column: A new column `datetime` is created by concatenating the `Date` and `Time` columns.
Deleting Rows with Invalid Timestamps: The rows in which the `datetime` column cannot be converted are deleted and the dataset is ordered on the `datetime` column to ensure chronology.  
Data Preparing for Streaming: JSON payload is created along with a timestamp to indicate when the data were taken and readings from sensors of significant pollutants and environmental parameters.
Time Synchronization Implementation: A delay function mimics real-time data flowing through the insertion of a proportionate time lag between successive records.
Error Handling: Error in data parsing or streaming is logged but never terminate the script's run.
Final Steps: Messages are published to Kafka prior to script exit.

**Data Preprocessing Method in Consumer Script:**
Message Validation: Verifies each message received possesses the anticipated fields: 'timestamp' and'sensor_data'.
Timestamp Conversion: Converts the 'timestamp' string to a Python 'datetime' object.
Sensor Data Type Conversion: Converts numeric values to standard form for analysis and plotting.
Database Schema Enforcement: Enforces a strict schema on the'measurements' table.
Parameterized Database Queries: Uses parameterized queries to write data to the database.
Error Handling: Captures and logs specific exceptions without stopping the consumer.
Real-Time Alerting: Adds a warning log entry when CO levels exceed over 50 µg/m³.

**Key Decisions:**
Enforced Strict Schema: Ensures data quality and easy querying.
Type Conversion: Prevents type errors during mathematical operations.
Parameterized Queries: Minimizes security risks and treats special characters appropriately.
Elegant Error Handling: Prevents cascading failures.
Real-Time Alerts: Aligns with environmental monitoring best practices.
