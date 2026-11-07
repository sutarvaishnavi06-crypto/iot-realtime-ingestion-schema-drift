import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "iot_warehouse.db"

def connect():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db

def initialize_database():
    with connect() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL, timestamp TEXT NOT NULL,
            temperature REAL NOT NULL, humidity REAL NOT NULL,
            pressure REAL, schema_version INTEGER NOT NULL,
            temperature_status TEXT NOT NULL, raw_json TEXT NOT NULL,
            ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS invalid_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_json TEXT, error_message TEXT NOT NULL,
            received_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS schema_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            schema_version INTEGER NOT NULL, fields TEXT NOT NULL,
            change_description TEXT NOT NULL,
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

def save_valid(record, raw):
    with connect() as db:
        db.execute("""INSERT INTO sensor_readings
        (device_id,timestamp,temperature,humidity,pressure,schema_version,
         temperature_status,raw_json) VALUES (?,?,?,?,?,?,?,?)""",
        (record["device_id"], record["timestamp"], record["temperature"],
         record["humidity"], record["pressure"], record["schema_version"],
         record["temperature_status"], json.dumps(raw)))

def save_invalid(raw, error):
    with connect() as db:
        db.execute("INSERT INTO invalid_records(raw_json,error_message) VALUES(?,?)",
                   (json.dumps(raw, default=str), str(error)))

def save_schema_change(version, fields, description):
    with connect() as db:
        db.execute("INSERT INTO schema_history(schema_version,fields,change_description) VALUES(?,?,?)",
                   (version, json.dumps(sorted(fields)), description))

def get_statistics():
    with connect() as db:
        return {
            "processed": db.execute("SELECT COUNT(*) FROM sensor_readings").fetchone()[0],
            "invalid": db.execute("SELECT COUNT(*) FROM invalid_records").fetchone()[0],
            "schema_changes": db.execute("SELECT COUNT(*) FROM schema_history").fetchone()[0],
        }
