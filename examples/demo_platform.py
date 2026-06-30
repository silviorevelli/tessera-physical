"""
Demo: Tessera as a real shared platform — one server, multiple clients.

Starts a Flight server over a Lake (with an API key), then two independent clients:
  - client A writes telemetry for two runs,
  - client B (a different connection) queries and runs the catalog.

This is the multi-client, networked scenario a library alone can't cover.
"""

import math
import shutil
import threading
import time
from pathlib import Path

from tessera import IMU, Lake, TesseraClient, TesseraServer, Vec3

LAKE = Path(__file__).resolve().parent.parent / "datalake_platform"
LOCATION = "grpc://127.0.0.1:8821"
API_KEY = "secret-key-123"


def make_run(bump_at):
    out = []
    for i in range(200):
        t = 1_700_000_000_000_000_000 + i * 10_000_000
        ax = 7.5 if (bump_at is not None and abs(i - bump_at) < 3) else 0.2 * math.sin(i / 10)
        out.append(IMU(t, Vec3(ax, 0.1, 9.81), Vec3(0.0, 0.0, 0.05)))
    return out


def main():
    shutil.rmtree(LAKE, ignore_errors=True)

    server = TesseraServer(Lake(LAKE), location="grpc://0.0.0.0:8821", api_keys={API_KEY})
    threading.Thread(target=server.serve, daemon=True).start()
    time.sleep(0.5)  # let the server bind
    print(f"server up at {LOCATION} (auth required)")

    # --- client A: writer ---
    a = TesseraClient(LOCATION, api_key=API_KEY)
    print("client A  ping:", a.ping())
    a.write("run01", "imu", make_run(None))
    a.write("run02", "imu", make_run(120))
    print("client A  wrote run01, run02")

    # --- client B: reader (separate connection) ---
    b = TesseraClient(LOCATION, api_key=API_KEY)
    print("client B  topics:", b.topics(), " sequences:", b.sequences("imu"))

    cat = b.catalog("imu", "acceleration.x > 5")
    print("client B  catalog (acceleration.x > 5):")
    print(cat.to_pandas().to_string(index=False))

    hits = b.find("imu", "acceleration.x > 5",
                  columns=["sequence", "timestamp_ns", "acceleration.x"], limit=3)
    print("client B  first matches:")
    print(hits.to_pandas().to_string(index=False))

    # --- auth rejection ---
    try:
        TesseraClient(LOCATION, api_key="wrong").ping()
        print("ERROR: bad key was accepted")
    except Exception as e:
        print("bad API key correctly rejected:", type(e).__name__)

    server.shutdown()
    print("OK")


if __name__ == "__main__":
    main()
