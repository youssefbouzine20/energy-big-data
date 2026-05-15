import json
import os
import tempfile
from pathlib import Path

STATE_FILE = Path(__file__).parent / ".weather_state.json"


def set_weather(severity: str, temperature: float) -> None:
    data = {"weather_severity": severity, "temperature_c": temperature}
    fd, tmp_path = tempfile.mkstemp(
        dir=STATE_FILE.parent, prefix=".weather_state.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise


def get_weather() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"weather_severity": "NORMAL", "temperature_c": 26.0}
