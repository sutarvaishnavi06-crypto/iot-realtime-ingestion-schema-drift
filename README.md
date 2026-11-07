# Real-Time IoT Data Ingestion Pipeline with Schema Drift Handling

A simple Python assignment demonstrating Kafka streaming, schema evolution,
JSON transformation, data quality checks, SQLite storage, and terminal monitoring.

## Requirements
- Python 3.9+
- Docker Desktop with Docker Compose

## Run on Windows (PowerShell)

1. Open this folder in VS Code and install the dependency:

   `python -m pip install -r requirements.txt`

2. Start Docker Desktop, then start Kafka:

   `docker compose up -d`

3. Create the topic (safe to run more than once):

   `docker exec -it iot-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic iot-sensor-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

4. In terminal 1, start the consumer:

   `python consumer.py`

5. In terminal 2, start the producer:

   `python producer.py`

The first five events use schema v1 (temperature and humidity).
From event six onward, schema v2 adds pressure. The consumer detects the
field change, validates/transforms records and stores them in SQLite.

To send a finite batch instead of running continuously:

   `python producer.py --count 12`

Press Ctrl+C to stop either process. The consumer prints a monitoring summary.

## SQLite tables
- `sensor_readings`: valid, transformed, analytics-ready records.
- `invalid_records`: invalid messages and their error reasons.
- `schema_history`: detected schema field changes.

The database `iot_warehouse.db` is created automatically in this folder.

## Assignment requirement coverage
- Streaming consumer: Apache Kafka consumer.
- Schema management: schema versions, required-field checks and field drift detection.
- Transformation: field normalization, type casting and enrichment.
- Data quality: range checks and invalid-record handling.
- Warehouse sink: SQLite.
- Monitoring: terminal logs and persisted schema/data-quality counts.

This is a local single-broker educational implementation, not a production cluster.
