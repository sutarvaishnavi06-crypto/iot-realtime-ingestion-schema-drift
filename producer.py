import argparse, json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "iot-sensor-data"

def make_event(number):
    version = 1 if number <= 5 else 2
    event = {
        "device_id": f"IOT-{random.randint(1, 3):03d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(18, 42), 2),
        "humidity": round(random.uniform(25, 85), 2),
        "schema_version": version,
    }
    if version == 2:
        event["pressure"] = round(random.uniform(990, 1030), 2)
    return event

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=0, help="0 = run continuously")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    if args.count < 0 or args.interval < 0:
        parser.error("count and interval must be non-negative")
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all", retries=5
    )
    number = 0
    print(f"Sending IoT events to {TOPIC}; first 5 use v1, then v2.")
    try:
        while args.count == 0 or number < args.count:
            number += 1
            event = make_event(number)
            producer.send(TOPIC, event).get(timeout=10)
            print("SENT:", json.dumps(event))
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
