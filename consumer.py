import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from schema_manager import inspect_schema, SchemaError
from transformer import transform_event, DataQualityError
from warehouse import initialize_database, save_valid, save_invalid, save_schema_change
from monitoring import log_status, print_summary

TOPIC = "iot-sensor-data"

def main():
    initialize_database()
    consumer = KafkaConsumer(
        TOPIC, bootstrap_servers="localhost:9092",
        group_id="iot-ingestion-consumer", auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: value.decode("utf-8")
    )
    known_fields = set()
    received = 0
    print(f"Consumer listening on {TOPIC}. Start producer in another terminal.")
    try:
        for message in consumer:
            received += 1
            raw_text, raw = message.value, None
            try:
                raw = json.loads(raw_text)
                version, added, absent = inspect_schema(raw)
                fields = set(raw)
                if fields != known_fields:
                    description = f"Added fields: {sorted(added)}; optional fields absent: {sorted(absent)}"
                    save_schema_change(version, fields, description)
                    log_status("SCHEMA CHANGE", description)
                    known_fields = fields
                record = transform_event(raw)
                save_valid(record, raw)
                log_status("PROCESSED", f"device={record['device_id']} v{version}, "
                           f"temperature={record['temperature']}, humidity={record['humidity']}, "
                           f"pressure={record['pressure']}, status={record['temperature_status']}")
            except (json.JSONDecodeError, SchemaError, DataQualityError) as error:
                save_invalid(raw if raw is not None else raw_text, error)
                log_status("DATA QUALITY ERROR", str(error))
            except (TypeError, ValueError, KeyError) as error:
                save_invalid(raw if raw is not None else raw_text, error)
                log_status("PROCESSING ERROR", str(error))
            if received % 10 == 0:
                print_summary()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    except KafkaError as error:
        log_status("KAFKA ERROR", str(error))
    finally:
        consumer.close()
        print_summary()
        print("Consumer stopped safely.")

if __name__ == "__main__":
    main()
