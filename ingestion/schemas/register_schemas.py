"""
Registers all 3 JSON schemas with Confluent Schema Registry.

Runs once after `docker compose up`. Idempotent — re-running upgrades to a new
version only if the schema content has changed.

The producers do NOT depend on the registry to run — they still validate
client-side via the `jsonschema` library against the local schema files.
The registry serves as the team's central schema catalog visible in Kafka UI.

Run:
    .venv\\Scripts\\python -m ingestion.schemas.register_schemas      (Windows)
    .venv/bin/python -m ingestion.schemas.register_schemas            (Linux/WSL)
"""

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMAS_DIR = Path(__file__).resolve().parent

# Topic → schema file mapping. The "-value" suffix follows Confluent's subject
# naming convention (TopicNameStrategy): <topic>-value for record values.
SUBJECT_TO_SCHEMA = {
    "smart-meters-value":     "smart_meter.schema.json",
    "weather-value":          "weather.schema.json",
    "incident-reports-value": "incident_report.schema.json",
}


def register(subject: str, schema: dict) -> None:
    body = json.dumps({
        "schemaType": "JSON",
        "schema": json.dumps(schema),
    }).encode("utf-8")

    req = urllib.request.Request(
        url=f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        data=body,
        method="POST",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            print(f"[OK]    {subject:<30} id={result['id']}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        print(f"[ERROR] {subject:<30} HTTP {e.code}: {body}")
    except urllib.error.URLError as e:
        print(f"[ERROR] {subject:<30} cannot reach {SCHEMA_REGISTRY_URL}: {e.reason}")


def main() -> int:
    print(f"[INFO] Registering schemas at {SCHEMA_REGISTRY_URL}\n")
    for subject, filename in SUBJECT_TO_SCHEMA.items():
        schema_path = SCHEMAS_DIR / filename
        if not schema_path.exists():
            print(f"[SKIP]  {subject:<30} file not found: {schema_path}")
            continue
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        register(subject, schema)
    print(f"\n[INFO] Done. View in Kafka UI: http://localhost:{os.getenv('KAFKA_UI_PORT', '8090')}/ui/clusters/.../schemas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
