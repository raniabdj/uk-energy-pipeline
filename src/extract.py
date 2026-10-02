"""
Bronze layer: extract raw data from the UK Carbon Intensity API
(https://carbonintensity.org.uk/) and land it as-is, untouched.

No API key required — this is a free, public UK government-backed API.
Run this on a schedule (cron, Azure Data Factory, GitHub Actions, etc.)
to build up a real historical dataset over time.
"""
import json
import os
from datetime import datetime, timezone

import requests

BASE_URL = "https://api.carbonintensity.org.uk"
BRONZE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bronze")


def fetch(endpoint: str) -> dict:
    """GET a Carbon Intensity API endpoint and return the parsed JSON."""
    resp = requests.get(f"{BASE_URL}{endpoint}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def save_raw(payload: dict, name: str) -> str:
    """Land raw JSON exactly as received, timestamped — this is the bronze layer."""
    os.makedirs(BRONZE_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(BRONZE_DIR, f"{name}_{stamp}.json")
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)
    return path


def main():
    # National carbon intensity, last 48 half-hour settlement periods
    intensity = fetch("/intensity")
    save_raw(intensity, "intensity")

    # National generation mix (gas, wind, nuclear, solar, etc.) right now
    generation_mix = fetch("/generation")
    save_raw(generation_mix, "generation_mix")

    # Regional breakdown — 17 GB regions, intensity + generation mix per region
    regional = fetch("/regional")
    save_raw(regional, "regional")

    print("Extract complete — raw payloads landed in data/bronze/")


if __name__ == "__main__":
    main()
