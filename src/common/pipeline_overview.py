import json
from datetime import datetime
from pathlib import Path

PIPELINE_META = {
    "name": "SPTrans Data Pipeline",
    "version": "2.0.0",
    "layers": ["raw", "silver", "gold"],
    "update_frequency": "2min",
    "data_sources": ["SPTrans API", "GTFS"],
    "tools": ["Airflow", "Spark", "MinIO", "Postgres", "Grafana"]
}

def generate_summary():
    timestamp = datetime.utcnow().isoformat()
    summary = {
        "timestamp": timestamp,
        "pipeline": PIPELINE_META,
        "status": "operational"
    }
    output_path = Path("docs/pipeline_summary.json")
    output_path.write_text(json.dumps(summary, indent=2))
    print(f"✅ Pipeline summary generated at {output_path}")

if __name__ == "__main__":
    generate_summary()
