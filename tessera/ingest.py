"""
CSV ingest — read CSV files into the lake.

This module:

  1. reads the CSV with pyarrow's typed parser (infers int/float/string),
  2. normalizes the time column into `timestamp_ns` (int64),
  3. passes it to `Lake.write_table` — the same write path as in-memory records.

The result is the same Parquet part files that `lake.write([...])` would produce, so
there is no separate format or write path.

    from tessera import Lake
    from tessera.ingest import ingest_csv

    lake = Lake("./datalake")
    # CSV with columns: t,acc_x,acc_y,acc_z  (t in seconds)
    ingest_csv(
        lake, "run01", "imu", "imu.csv",
        timestamp="t", unit="s",
        rename={"acc_x": "acceleration.x",
                "acc_y": "acceleration.y",
                "acc_z": "acceleration.z"},
    )

For files larger than RAM, pass `batch_rows=...` to read the file in chunks, each
written as a separate part file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence as Seq

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from .lake import TIMESTAMP, Lake

# Conversion factor to nanoseconds for each time unit.
_UNIT_NS = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}


def read_csv(
    path: str | Path,
    *,
    timestamp: str = TIMESTAMP,
    unit: str = "ns",
    rename: Mapping[str, str] | None = None,
    columns: Seq[str] | None = None,
    delimiter: str = ",",
    null_values: Seq[str] | None = None,
    column_names: Seq[str] | None = None,
    skip_rows: int = 0,
) -> pa.Table:
    """Read a CSV into a typed Arrow table, ready for the lake.

    Args:
        path: path to the CSV file.
        timestamp: name of the column holding the time. It is converted to int64
            nanoseconds and renamed `timestamp_ns` (default: it already is).
        unit: unit of the time column: 'ns' | 'us' | 'ms' | 's'.
        rename: map of CSV header -> lake column name, applied FIRST (so `timestamp`
            and `columns` refer to the final names). Mostly for dotted names:
            {"acc_x": "acceleration.x"}.
        columns: if given, keep only these columns (plus `timestamp_ns`).
        delimiter: field separator (e.g. ';' or '\\t').
        null_values: strings to treat as missing (in addition to "").
        column_names: override the column names instead of reading them from the
            file's header. Use for headerless files, or for broken headers (e.g. a
            duplicated/extra header column that doesn't match the data) — pair it
            with `skip_rows=1` to drop the bogus header row.
        skip_rows: rows to skip at the start of the file before reading.

    Rows with a missing/invalid time are dropped (a sample with no time can't be
    placed on any sequence).
    """
    if unit not in _UNIT_NS:
        raise ValueError(f"invalid unit '{unit}': use one of {sorted(_UNIT_NS)}")

    read, parse, convert = _options(delimiter, null_values, column_names, skip_rows)
    table = pacsv.read_csv(
        str(path), read_options=read, parse_options=parse, convert_options=convert
    )
    return _prepare(table, timestamp, unit, rename, columns)


def ingest_csv(
    lake: Lake,
    sequence: str,
    topic: str,
    path: str | Path,
    *,
    timestamp: str = TIMESTAMP,
    unit: str = "ns",
    rename: Mapping[str, str] | None = None,
    columns: Seq[str] | None = None,
    delimiter: str = ",",
    null_values: Seq[str] | None = None,
    column_names: Seq[str] | None = None,
    skip_rows: int = 0,
    batch_rows: int | None = None,
    row_group_size: int = 64_000,
) -> list[str]:
    """Read a CSV and write it into the lake. Returns the part file paths created.

    The read arguments are those of `read_csv`. In addition:
        batch_rows: if given, the CSV is read in chunks of about this many rows and
            each chunk becomes a distinct part file. This lets a file larger than
            RAM enter the lake without materializing it all at once.
        row_group_size: row group size in the written Parquet.
    """
    if batch_rows is None:
        table = read_csv(
            path, timestamp=timestamp, unit=unit, rename=rename,
            columns=columns, delimiter=delimiter, null_values=null_values,
            column_names=column_names, skip_rows=skip_rows,
        )
        return [lake.write_table(sequence, topic, table, row_group_size=row_group_size)]

    if batch_rows <= 0:
        raise ValueError("batch_rows must be > 0")

    read, parse, convert = _options(delimiter, null_values, column_names, skip_rows)
    reader = pacsv.open_csv(
        str(path), read_options=read, parse_options=parse, convert_options=convert
    )

    paths: list[str] = []
    buffer: list[pa.RecordBatch] = []   # ALREADY-prepared batches (timestamp_ns, renamed)
    buffered = 0

    def flush(force: bool) -> None:
        """Write part files of `batch_rows` rows. If `force`, also write the remainder."""
        nonlocal buffer, buffered
        if not buffered:
            return
        table = pa.Table.from_batches(buffer)
        offset = 0
        while table.num_rows - offset >= batch_rows or (force and offset < table.num_rows):
            chunk = table.slice(offset, batch_rows)
            paths.append(lake.write_table(sequence, topic, chunk, row_group_size=row_group_size))
            offset += chunk.num_rows
        # keep the leftover rows (below the threshold) waiting for the next chunks.
        rest = table.slice(offset)
        buffer = rest.to_batches() if rest.num_rows else []
        buffered = rest.num_rows

    for batch in reader:
        # prepare each chunk right away: the source time column exists only before
        # the conversion to timestamp_ns.
        prepared = _prepare(pa.Table.from_batches([batch]), timestamp, unit, rename, columns)
        if prepared.num_rows:
            buffer.extend(prepared.to_batches())
            buffered += prepared.num_rows
        if buffered >= batch_rows:
            flush(force=False)
    flush(force=True)
    return paths


# --- internal --------------------------------------------------------------

def _options(
    delimiter: str,
    null_values: Seq[str] | None,
    column_names: Seq[str] | None,
    skip_rows: int,
) -> tuple[pacsv.ReadOptions, pacsv.ParseOptions, pacsv.ConvertOptions]:
    """Build the pyarrow CSV options, shared by the one-shot and streaming readers."""
    read = pacsv.ReadOptions(
        column_names=list(column_names) if column_names else None,
        skip_rows=skip_rows,
    )
    parse = pacsv.ParseOptions(delimiter=delimiter)
    convert = pacsv.ConvertOptions(
        null_values=list(null_values) if null_values else None,
        strings_can_be_null=True,
    )
    return read, parse, convert


def _prepare(
    table: pa.Table,
    timestamp: str,
    unit: str,
    rename: Mapping[str, str] | None,
    columns: Seq[str] | None,
) -> pa.Table:
    """Rename -> normalize the time into `timestamp_ns` -> select the columns."""
    if rename:
        table = table.rename_columns([rename.get(n, n) for n in table.column_names])

    if timestamp not in table.column_names:
        raise ValueError(
            f"timestamp column '{timestamp}' absent; available columns: {table.column_names}"
        )

    table = _to_timestamp_ns(table, timestamp, unit)
    table = table.filter(pc.is_valid(table.column(TIMESTAMP)))   # drop rows with no time

    if columns is not None:
        keep = [TIMESTAMP] + [c for c in columns if c != TIMESTAMP]
        missing = [c for c in keep if c not in table.column_names]
        if missing:
            raise ValueError(f"requested columns absent in the CSV: {missing}")
        table = table.select(keep)

    return table


def _to_timestamp_ns(table: pa.Table, src: str, unit: str) -> pa.Table:
    """Convert column `src` to int64 nanoseconds and rename it `timestamp_ns`."""
    idx = table.column_names.index(src)
    col = table.column(src)
    factor = _UNIT_NS[unit]

    if pa.types.is_timestamp(col.type):
        # Arrow timestamp (e.g. the parser recognized an ISO date): -> epoch ns
        ns = pc.cast(col, pa.timestamp("ns")).cast(pa.int64())
    elif pa.types.is_integer(col.type):
        # integers: multiply in int64, NOT in float (an epoch in ns exceeds the
        # 2^53 exactly representable in float64 and would lose precision).
        ns = pc.cast(col, pa.int64())
        if factor != 1:
            ns = pc.multiply(ns, pa.scalar(factor, pa.int64()))
    else:
        # float (e.g. seconds with decimals): scale then truncate to int64. The
        # scaled value usually carries a tiny fractional part (float rounding), so
        # the int64 cast must be allowed to truncate it instead of erroring.
        scaled = pc.multiply(pc.cast(col, pa.float64()), float(factor))
        ns = pc.cast(scaled, options=pc.CastOptions(target_type=pa.int64(),
                                                    allow_float_truncate=True))

    return table.set_column(idx, TIMESTAMP, ns)
