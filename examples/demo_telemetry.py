"""
Demo: two mobile-robot telemetry runs.
Writes GPS + IMU + odometry, then searches by physical value.
"""

import math
import shutil
from pathlib import Path

from tessera import GPS, IMU, Lake, Vec3, WheelOdometry

LAKE = Path(__file__).resolve().parent.parent / "datalake"


def make_run(name: str, bump_at: int | None):
    """Generate 200 samples. If bump_at is given, at that index there's a shake
    (high lateral acceleration) and a steep slope."""
    imu, gps, odo = [], [], []
    for i in range(200):
        t = 1_700_000_000_000_000_000 + i * 10_000_000  # 100 Hz
        ax = 0.2 * math.sin(i / 10)
        slope = 3.0
        if bump_at is not None and abs(i - bump_at) < 3:
            ax = 7.5            # shake: |acc.x| > 5
            slope = 18.0        # steep slope
        imu.append(IMU(t, Vec3(ax, 0.1, 9.81), Vec3(0.0, 0.0, 0.05)))
        gps.append(GPS(t, 45.07 + i * 1e-5, 7.68 + i * 1e-5, 240.0,
                       satellites=9, hdop=0.8))
        odo.append(WheelOdometry(t, speed_mps=0.4, heading_rad=0.0,
                                 motor_current_a=3.0 if slope < 10 else 11.0,
                                 slope_deg=slope))
    return imu, gps, odo


def main():
    shutil.rmtree(LAKE, ignore_errors=True)  # idempotent demo (write appends)
    lake = Lake(LAKE)
    for run, bump in [("run01", None), ("run02", 120)]:
        imu, gps, odo = make_run(run, bump)
        lake.write(run, "imu", imu)
        lake.write(run, "gps", gps)
        lake.write(run, "odometry", odo)

    print("topics :", lake.topics())
    print("runs   :", lake.sequences("imu"))

    print("\n# Catalog: in which runs does lateral acceleration exceed 5 m/s^2?")
    print(lake.catalog("imu", "acceleration.x > 5").to_pandas().to_string(index=False))

    print("\n# The exact rows of the shake (search by physical value):")
    hits = lake.find("imu", "acceleration.x > 5",
                     columns=["sequence", "timestamp_ns", "acceleration.x"])
    print(hits.to_pandas().to_string(index=False))

    print("\n# Logical join: motor current when the slope exceeds 15 degrees")
    print(lake.find("odometry", "slope_deg > 15",
                    columns=["sequence", "slope_deg", "motor_current_a"]
                    ).to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
