Real-Time Weather Data Pipeline
(Kafka → Spark Structured Streaming → PostgreSQL)

This project demonstrates a complete real-time data engineering pipeline using:

Apache Kafka for streaming ingestion

Spark Structured Streaming for real-time processing

PostgreSQL for storage

Python for orchestration

It reads live weather data from an API, publishes to Kafka, processes using Spark, and stores results into PostgreSQL.

📌 Features

✔ Real-time weather data ingestion
✔ Kafka producer & consumer architecture
✔ Spark Structured Streaming transformation
✔ JDBC write into PostgreSQL
✔ Fully automated streaming workflow
✔ Ready for deployment & extension

🛠️ Tech Stack
Component	Technology
Stream Producer	Python + Kafka
Stream Processor	Apache Spark 3.5.1
Message Broker	Apache Kafka
Database	PostgreSQL
Runtime	WSL2 + Windows
📂 Project Structure
weather_data_pipeline/
│
├── producer.py              # Fetch weather data → Kafka
├── spark_stream.py          # Spark Streaming (Kafka → Postgres)
├── requirements.txt         # Python dependencies
├── README.md                # Documentation

🚀 How to Run This Project

Follow these steps carefully.

✅ 1. Start Zookeeper & Kafka
Open WSL terminal → go to Kafka directory
cd /mnt/c/kafka/kafka_2.12-3.2.3

Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

Open new terminal → Start Kafka broker
cd /mnt/c/kafka/kafka_2.12-3.2.3
bin/kafka-server-start.sh config/server.properties

✅ 2. Create Kafka Topic
cd /mnt/c/kafka/kafka_2.12-3.2.3

bin/kafka-topics.sh --create \
  --topic weather_topic \
  --bootstrap-server localhost:9092


Verify:

bin/kafka-topics.sh --list --bootstrap-server localhost:9092

✅ 3. Start PostgreSQL
Start server
sudo service postgresql start

Enter psql
sudo -u postgres psql

Create database
CREATE DATABASE clickstream_db;

Connect to DB
\c clickstream_db;

Create table
CREATE TABLE weather_stream (
    lon FLOAT,
    lat FLOAT,
    temperature FLOAT,
    humidity INT,
    city VARCHAR(50),
    batch_time TIMESTAMP
);

✅ 4. Install Dependencies

Inside your project folder:

pip install -r requirements.txt

✅ 5. Start Kafka Weather Producer

This script fetches live weather data and sends it to Kafka every few seconds.

python3 producer.py

✅ 6. Run Spark Streaming Job

Go to Spark folder:

cd ~/spark-3.5.1-bin-hadoop3


Run with PostgreSQL + Kafka JARs:

./bin/spark-submit \
  --jars jars-kafka/*,jars-kafka/postgresql-42.7.2.jar \
  "/mnt/c/Users/asus/OneDrive/Desktop/data pipeline/weather_data_pipeline/spark_stream.py"


Spark UI → http://localhost:4040

🎯 Output

✔ Weather data is produced to Kafka
✔ Spark consumes it in real-time
✔ JSON is transformed into columns
✔ Final data is stored in PostgreSQL table weather_stream

View results:
SELECT * FROM weather_stream;

📦 requirements.txt
pyspark
requests
kafka-python
psycopg2-binary

🔮 Future Enhancements

Add Apache Airflow orchestration

Migrate storage to Snowflake

Containerize with Docker

Add Power BI Dashboards

✨ Author

Vivek Kumar
Data Engineer — Spark | Kafka | PostgreSQL | Azure
