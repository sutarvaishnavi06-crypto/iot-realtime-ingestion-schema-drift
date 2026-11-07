REQUIRED_FIELDS = {"device_id", "timestamp", "temperature", "humidity"}
OPTIONAL_FIELDS = {"pressure"}
KNOWN_FIELDS = REQUIRED_FIELDS | OPTIONAL_FIELDS | {"schema_version"}

class SchemaError(ValueError):
    pass

def inspect_schema(event):
    if not isinstance(event, dict):
        raise SchemaError("Message must be a JSON object.")
    missing = REQUIRED_FIELDS - set(event)
    if missing:
        raise SchemaError(f"Missing required fields: {sorted(missing)}")
    try:
        version = int(event.get("schema_version", 1))
    except (TypeError, ValueError):
        raise SchemaError("schema_version must be an integer.")
    if version not in (1, 2):
        raise SchemaError(f"Unsupported schema version: {version}")
    added = set(event) - KNOWN_FIELDS
    optional_absent = OPTIONAL_FIELDS - set(event)
    return version, added, optional_absent
