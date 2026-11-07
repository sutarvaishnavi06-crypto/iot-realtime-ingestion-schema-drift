from warehouse import get_statistics

def log_status(status, message):
    print(f"[{status}] {message}")

def print_summary():
    stats = get_statistics()
    print("\n--- PIPELINE MONITORING ---")
    print(f"Processed records : {stats['processed']}")
    print(f"Invalid records   : {stats['invalid']}")
    print(f"Schema changes    : {stats['schema_changes']}")
    print("---------------------------\n")
