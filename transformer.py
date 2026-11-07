from datetime import datetime

class DataQualityError(ValueError):
    pass

def transform_event(event):
    record = {
        str(key).strip().lower(): value.strip() if isinstance(value, str) else value
        for key, value in event.items()
    }
    required = ("device_id", "timestamp", "temperature", "humidity")
    missing = [key for key in required if record.get(key) in (None, "")]
    if missing:
        raise DataQualityError(f"Missing required values: {missing}")
    try:
        record["device_id"] = str(record["device_id"])
        record["temperature"] = float(record["temperature"])
        record["humidity"] = float(record["humidity"])
        record["schema_version"] = int(record.get("schema_version", 1))
        datetime.fromisoformat(str(record["timestamp"]).replace("Z", "+00:00"))
        pressure = record.get("pressure")
        record["pressure"] = None if pressure in (None, "") else float(pressure)
    except (TypeError, ValueError) as error:
        raise DataQualityError(f"Invalid type or timestamp: {error}")
    if not -50 <= record["temperature"] <= 100:
        raise DataQualityError("Temperature must be between -50 and 100 C.")
    if not 0 <= record["humidity"] <= 100:
        raise DataQualityError("Humidity must be between 0 and 100%.")
    if record["pressure"] is not None and not 800 <= record["pressure"] <= 1200:
        raise DataQualityError("Pressure must be between 800 and 1200 hPa.")
    record["temperature_status"] = "HIGH" if record["temperature"] > 35 else "NORMAL"
    return record
