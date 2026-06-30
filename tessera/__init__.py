"""
Tessera — a sensor-data layer on Apache Parquet and Apache DataFusion.

    from tessera import Lake, IMU, GPS, Vec3

    lake = Lake("./datalake")
    lake.write("run01", "imu", [IMU(t, Vec3(...), Vec3(...)) for t in ...])
    lake.find("imu", "acceleration.x > 5")        # query by physical value
    lake.catalog("imu", "acceleration.x > 5")     # which sequences contain it
"""

from .client import TesseraClient
from .ingest import ingest_csv, read_csv
from .lake import Lake
from .ontology import GPS, IMU, Sensor, Vec3, WheelOdometry, flatten
from .server import TesseraServer
from .sync import align

__all__ = [
    "Lake", "Sensor", "Vec3", "IMU", "GPS", "WheelOdometry", "flatten", "align",
    "read_csv", "ingest_csv",
    "TesseraServer", "TesseraClient",
]
