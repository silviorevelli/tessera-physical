"""
Typed ontology.

Sensors are described by small dataclasses declaring their fields (e.g. a GPS has
latitude/longitude/altitude, an IMU has acceleration.x/y/z). Each dataclass can
flatten itself into dotted-name columns ("acceleration.x"), which Parquet stores as
ordinary columns and DataFusion filters with pushdown.

Adding a new sensor type means writing a dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, is_dataclass
from typing import Any


def flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Recursively flatten a dataclass into {dotted_column: value}.

    Vec3(x=1, y=2, z=3) under 'acceleration' -> {'acceleration.x': 1, ...}
    This is all it takes for "search by physical value" to work: the value ends up
    in a Parquet column, indexed by design.
    """
    out: dict[str, Any] = {}
    for f in fields(obj):
        val = getattr(obj, f.name)
        key = f"{prefix}{f.name}"
        if is_dataclass(val) and not isinstance(val, type):
            out.update(flatten(val, prefix=f"{key}."))
        else:
            out[key] = val
    return out


class Sensor:
    """Common base: every sensor knows how to flatten into a row of columns."""

    def row(self) -> dict[str, Any]:
        return flatten(self)


# --- Reusable value types -------------------------------------------------

@dataclass
class Vec3(Sensor):
    x: float
    y: float
    z: float


# --- Sensors (the ontology proper) -----------------------------------------
# Every record carries 'timestamp_ns' so the row is self-contained.

@dataclass
class IMU(Sensor):
    timestamp_ns: int
    acceleration: Vec3          # m/s^2   -> columns acceleration.x/.y/.z
    angular_velocity: Vec3      # rad/s   -> angular_velocity.x/.y/.z


@dataclass
class GPS(Sensor):
    timestamp_ns: int
    latitude: float
    longitude: float
    altitude: float
    satellites: int = 0
    hdop: float = 99.0          # Horizontal Dilution of Precision (lower = better)


@dataclass
class WheelOdometry(Sensor):
    """Typical of a mobile robot: wheel speed + motor current."""
    timestamp_ns: int
    speed_mps: float
    heading_rad: float
    motor_current_a: float
    slope_deg: float = 0.0
