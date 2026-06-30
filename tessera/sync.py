"""
Synchronization — align sensors at different rates onto a fixed grid.

Sensors are often sampled at different, out-of-phase rates (e.g. GPS at ~5 Hz, IMU at
~100 Hz, odometry at 50 Hz). Model training usually needs a dense, fixed-frequency
table: one row every grid step, all columns filled.

The operation is an as-of join: for each grid tick, take the last sample of each
sensor. It is implemented as a DataFusion window function:

    LAST_VALUE(col) IGNORE NULLS OVER (ORDER BY ts ...)   ->  zero-order hold

It operates on in-memory tables, so it is independent of where the data is stored
(local disk or object store).
"""

from __future__ import annotations

from typing import Sequence as Seq

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from datafusion import SessionContext

_META = {"sequence", "topic", "timestamp_ns"}


def align(
    src: dict[str, pa.Table],
    hz: float,
    *,
    columns: dict[str, Seq[str]] | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> pa.Table:
    """Align several topics (already read as tables) onto a fixed `hz` grid.

    `src` maps topic -> table (with `timestamp_ns` + feature columns).
    For each grid tick, every column takes the last known value of its sensor
    (zero-order hold). Output columns are prefixed with the topic:
    `imu.acceleration.x`, `gps.latitude`, ...
    """
    # 1) Collect (type, topic, column) for each feature, and the time spans.
    feat: dict[str, tuple[pa.DataType, str, str]] = {}
    spans: list[tuple[int, int]] = []
    for topic, tbl in src.items():
        cols = (columns or {}).get(topic) or [c for c in tbl.column_names if c not in _META]
        lo_hi = pc.min_max(tbl.column("timestamp_ns"))
        spans.append((lo_hi["min"].as_py(), lo_hi["max"].as_py()))
        for c in cols:
            feat[f"{topic}.{c}"] = (tbl.schema.field(c).type, topic, c)
    out = list(feat)

    # 2) Grid: the interval in which ALL sensors have already started (no gaps).
    lo = max(s[0] for s in spans) if start_ns is None else start_ns
    hi = min(s[1] for s in spans) if end_ns is None else end_ns
    step = int(round(1e9 / hz))
    grid = np.arange(lo, hi + 1, step, dtype="int64")

    # 3) Stack grid + samples into a single time-ordered relation.
    #    ord=0 for samples, ord=1 for ticks: at equal ts the sample comes "before"
    #    the tick, so a value arriving exactly on the tick is visible.
    def piece(ts_arr, ordv, filler) -> pa.Table:
        d = {"ts": pa.array(ts_arr, pa.int64()),
             "ord": pa.array(np.full(len(ts_arr), ordv, "int8"))}
        for name, (typ, topic, col) in feat.items():
            d[name] = filler(name, typ, topic, col, len(ts_arr))
        return pa.table(d)

    parts = [piece(grid, 1, lambda n, t, tp, c, k: pa.nulls(k, t))]  # grid: null features
    for topic, tbl in src.items():
        ts = tbl.column("timestamp_ns").to_numpy()
        parts.append(piece(
            ts, 0,
            lambda name, typ, tp, col, k, _tbl=tbl, _topic=topic:
                _tbl.column(col) if tp == _topic else pa.nulls(k, typ),
        ))
    unified = pa.concat_tables(parts)

    # 4) Zero-order hold via DataFusion: forward-fill every column, then keep the ticks.
    ctx = SessionContext()
    ctx.register_record_batches("u", [unified.to_batches()])
    win = "OVER (ORDER BY ts, ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    ff = ", ".join(f'LAST_VALUE("{c}") IGNORE NULLS {win} AS "{c}"' for c in out)
    sql = f"""
        SELECT timestamp_ns, {", ".join(f'"{c}"' for c in out)}
        FROM (
            SELECT ts AS timestamp_ns, ord, {ff}
            FROM u
        )
        WHERE ord = 1
        ORDER BY timestamp_ns
    """
    return ctx.sql(sql).to_arrow_table()
