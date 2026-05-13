import json
from pathlib import Path

STATE_FILE = Path(__file__).parent / ".weather_state.json"

def set_weather(severity: str, temperature: float):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "weather_severity": severity,
            "temperature_c":    temperature
        }, f)

def get_weather() -> dict:
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"weather_severity": "NORMAL", "temperature_c": 26.0}