"""
Demo: CSV ingest into the lake.

Shows that a sensor CSV (here an IMU exported with time in seconds and "their"
column names) becomes the same Parquet part files as `lake.write`, and is then
queryable by physical value like everything else.
"""

import shutil
import tempfile
from pathlib import Path

from tessera import Lake
from tessera.ingest import ingest_csv

LAKE = Path(__file__).resolve().parent.parent / "datalake"


def make_csv(path: Path) -> None:
    """Write a fake CSV: time in seconds, non-standard header, one jolt."""
    lines = ["t,acc_x,acc_y,acc_z"]
    t0 = 1_700_000_000.0
    for i in range(200):
        t = t0 + i * 0.01                       # 100 Hz, in SECONDS
        ax = 7.5 if abs(i - 120) < 3 else 0.2   # jolt around i=120
        lines.append(f"{t:.3f},{ax},0.1,9.81")
    path.write_text("\n".join(lines) + "\n")


def main():
    shutil.rmtree(LAKE, ignore_errors=True)
    lake = Lake(LAKE)

    with tempfile.TemporaryDirectory() as tmp:
        csv = Path(tmp) / "imu.csv"
        make_csv(csv)

        parts = ingest_csv(
            lake, "run01", "imu", csv,
            timestamp="t", unit="s",                # time in seconds -> timestamp_ns
            rename={"acc_x": "acceleration.x",      # "their" names -> dotted ontology
                    "acc_y": "acceleration.y",
                    "acc_z": "acceleration.z"},
            batch_rows=64,                          # read in chunks: several part files
        )
        print("part files written:", len(parts))

    print("topics :", lake.topics())
    print("runs   :", lake.sequences("imu"))

    print("\n# The jolt, searched by physical value (like native data):")
    hits = lake.find("imu", "acceleration.x > 5",
                     columns=["sequence", "timestamp_ns", "acceleration.x"])
    print(hits.to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
