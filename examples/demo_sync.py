"""
ML synchronization demo: fast IMU + slow GPS -> dense 50 Hz grid.

Typical scenario (mobile robot):
  - IMU  at 100 Hz
  - GPS  at   5 Hz
Out of phase. Training needs fixed-step rows with all columns filled.
"""

import shutil
from pathlib import Path

from tessera import GPS, IMU, Lake, Vec3

LAKE = Path(__file__).resolve().parent.parent / "datalake_sync"
T0 = 1_700_000_000_000_000_000


def main():
    shutil.rmtree(LAKE, ignore_errors=True)  # idempotent demo (write appends)
    lake = Lake(LAKE)

    # IMU at 100 Hz (10 ms step), 1 second -> 100 samples
    imu = [IMU(T0 + i * 10_000_000,
               acceleration=Vec3(0.01 * i, 0.0, 9.81),
               angular_velocity=Vec3(0.0, 0.0, 0.1))
           for i in range(100)]

    # GPS at 5 Hz (200 ms step), 1 second -> 5 samples
    gps = [GPS(T0 + i * 200_000_000,
               latitude=45.0 + i * 0.001, longitude=7.0 + i * 0.001,
               altitude=240.0, satellites=9, hdop=0.8)
           for i in range(5)]

    lake.write("run01", "imu", imu)
    lake.write("run01", "gps", gps)

    print(f"IMU: {len(imu)} samples @100Hz   GPS: {len(gps)} samples @5Hz")
    print("\n# Aligned onto a FIXED 50 Hz grid (20 ms step):")
    out = lake.align(
        "run01", ["imu", "gps"], hz=50,
        columns={"imu": ["acceleration.x"], "gps": ["latitude", "longitude"]},
    )
    df = out.to_pandas()
    df["t_ms"] = (df["timestamp_ns"] - T0) // 1_000_000
    cols = ["t_ms", "imu.acceleration.x", "gps.latitude", "gps.longitude"]
    print(df[cols].head(12).to_string(index=False))
    print(f"... {len(df)} total rows, constant density, no gaps.")
    print("\nNote: imu.acceleration.x changes every tick (fast sensor),")
    print("      gps.* stays 'held' until a new fix arrives (zero-order hold).")


if __name__ == "__main__":
    main()
