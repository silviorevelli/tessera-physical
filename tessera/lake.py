"""
The lake: write and query sensor data on Parquet + DataFusion.

It does two things:
  1. WRITE  -> Parquet files partitioned by (topic, sequence).
  2. QUERY  -> DataFusion, with pushdown, partition pruning, and disk spill.

Relevant behaviour:

  * Hive-partitioned layout:
        <root>/<topic>/sequence=<seq>/part-<uuid>.parquet
    The query engine prunes by partition, so a query on one sequence does not open
    the others. The same layout is readable by Spark, DuckDB, Polars, etc.

  * Append, not rewrite: every write adds a new immutable part file with a uuid name,
    so multiple writers can run in parallel without coordination.

  * Out-of-core: a bounded memory pool with disk spill lets sorts and aggregations
    over datasets larger than RAM spill to disk instead of failing.

  * Parallelism: multi-partition scan across the available cores.

  * Storage: `root` can be a local path or an object store (`s3://`, `gs://`,
    `az://`, `http(s)://`) via pyarrow.fs.

  * Streaming: `find_stream()` yields record batches, to consume results larger than
    RAM without materializing them.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Iterable, Iterator, Sequence as Seq

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from datafusion import RecordBatch, RuntimeEnvBuilder, SessionConfig, SessionContext

from .ontology import Sensor

TIMESTAMP = "timestamp_ns"
PART_COL = "sequence"

# Dotted identifier (acceleration.x): does NOT capture numbers (5.0).
_DOTTED = re.compile(r"\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+)\b")


def _quote_fields(where: str) -> str:
    """Quote dotted column names so SQL accepts them.

        acceleration.x > 5   ->   "acceleration.x" > 5
    """
    return _DOTTED.sub(r'"\1"', where)


class Lake:
    """A data lake on Parquet + DataFusion.

    Args:
        root: local folder (`./datalake`) or object store (`s3://bucket/prefix`,
            `gs://...`, `az://...`). Cloud needs credentials in the environment
            (e.g. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION).
        memory_limit_mb: memory cap of the query engine; beyond it, spill to disk.
        parallelism: target partitions for the parallel scan (default: core count).
    """

    def __init__(
        self,
        root: str | Path,
        *,
        memory_limit_mb: int = 512,
        parallelism: int | None = None,
    ):
        self.root = str(root).rstrip("/")
        self.remote = "://" in self.root
        if self.remote:
            self._fs, self._base = pafs.FileSystem.from_uri(self.root)
        else:
            p = Path(self.root).resolve()
            p.mkdir(parents=True, exist_ok=True)
            self._fs = pafs.LocalFileSystem()
            self._base = str(p).replace("\\", "/")
        self.memory_limit_mb = memory_limit_mb
        self.parallelism = parallelism or os.cpu_count() or 4

    # --- WRITE (append of immutable part files) ---------------------------

    def write(
        self,
        sequence: str,
        topic: str,
        records: Iterable[Sensor | dict],
        *,
        row_group_size: int = 64_000,
    ) -> str:
        """Append a part file to the (topic, sequence) partition. Rewrites nothing:
        multiple writes (even concurrent) accumulate distinct part files."""
        rows = [r.row() if isinstance(r, Sensor) else dict(r) for r in records]
        if not rows:
            raise ValueError("no records to write")
        for r in rows:
            if TIMESTAMP not in r:
                raise ValueError(f"missing '{TIMESTAMP}' in {r!r}")
        return self.write_table(
            sequence, topic, pa.Table.from_pylist(rows), row_group_size=row_group_size
        )

    def write_table(
        self,
        sequence: str,
        topic: str,
        table: pa.Table,
        *,
        row_group_size: int = 64_000,
    ) -> str:
        """Like `write`, but starting from an Arrow table (e.g. from a CSV or another
        lake). Same write path: one more immutable part file in the (topic, sequence)
        partition. Sorts by time and drops the partition column if it accidentally
        ended up among the data."""
        if table.num_rows == 0:
            raise ValueError("no records to write")
        if TIMESTAMP not in table.column_names:
            raise ValueError(f"missing column '{TIMESTAMP}' in the table")
        if PART_COL in table.column_names:   # the sequence lives in the path, not the data
            table = table.drop_columns([PART_COL])
        table = table.sort_by(TIMESTAMP)

        rel = f"{topic}/{PART_COL}={sequence}"
        name = f"part-{uuid.uuid4().hex}.parquet"
        if self.remote:
            dirp = f"{self._base}/{rel}"
            self._fs.create_dir(dirp, recursive=True)
            dest = f"{dirp}/{name}"
            pq.write_table(table, dest, filesystem=self._fs, row_group_size=row_group_size)
        else:
            d = Path(self._base) / rel
            d.mkdir(parents=True, exist_ok=True)
            dest = str(d / name)
            pq.write_table(table, dest, row_group_size=row_group_size)
        return dest

    # --- QUERY ENGINE (out-of-core) --------------------------------------

    def _ctx(self, topic: str) -> SessionContext:
        """DataFusion context with pushdown, partition pruning, parallelism, and
        disk spill (holds datasets larger than RAM)."""
        runtime = (
            RuntimeEnvBuilder()
            .with_disk_manager_os()
            .with_fair_spill_pool(self.memory_limit_mb * 1024 * 1024)
        )
        cfg = (
            SessionConfig()
            .set("datafusion.execution.parquet.pushdown_filters", "true")
            .set("datafusion.execution.parquet.reorder_filters", "true")
            .with_target_partitions(self.parallelism)
            .with_parquet_pruning(True)
        )
        ctx = SessionContext(cfg, runtime)
        if self.remote:
            self._register_object_store(ctx)
        url = self._table_url(topic)
        ctx.register_listing_table(
            topic, url, table_partition_cols=[(PART_COL, pa.string())]
        )
        return ctx

    def _table_url(self, topic: str) -> str:
        # The trailing slash is mandatory: it tells the object store "this is a
        # directory" (without it, it tries to open the path as a single file).
        if self.remote:
            return f"{self.root}/{topic}/"
        return Path(f"{self._base}/{topic}").as_uri() + "/"  # file:///... (Windows-safe)

    def _register_object_store(self, ctx: SessionContext) -> None:
        from datafusion.object_store import AmazonS3, GoogleCloud, Http, MicrosoftAzure

        scheme = self.root.split("://", 1)[0]
        bucket = self.root.split("://", 1)[1].split("/", 1)[0]
        if scheme == "s3":
            store = AmazonS3(bucket=bucket, region=os.environ.get("AWS_REGION"))
            ctx.register_object_store(f"s3://{bucket}", store)
        elif scheme in ("gs", "gcs"):
            ctx.register_object_store(f"gs://{bucket}", GoogleCloud(bucket=bucket))
        elif scheme in ("az", "abfs", "azure"):
            ctx.register_object_store(f"{scheme}://{bucket}", MicrosoftAzure(container=bucket))
        elif scheme in ("http", "https"):
            ctx.register_object_store(self.root, Http(self.root))

    def _build_sql(self, topic, where, columns, sequences, order, limit) -> str:
        sel = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        sql = f'SELECT {sel} FROM "{topic}"'
        clauses = []
        if sequences:
            joined = ", ".join(f"'{s}'" for s in sequences)
            clauses.append(f"{PART_COL} IN ({joined})")   # -> partition pruning
        if where:
            clauses.append(f"({_quote_fields(where)})")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        if order:
            sql += f" ORDER BY {TIMESTAMP}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return sql

    def find(
        self,
        topic: str,
        where: str | None = None,
        *,
        columns: Seq[str] | None = None,
        sequences: Seq[str] | None = None,
        order: bool = True,
        limit: int | None = None,
    ) -> pa.Table:
        """Find rows in a topic (all sequences, or those in `sequences`).
        Passing `sequences` prunes partitions: it doesn't open the others' files."""
        ctx = self._ctx(topic)
        return ctx.sql(self._build_sql(topic, where, columns, sequences, order, limit)).to_arrow_table()

    def find_stream(
        self,
        topic: str,
        where: str | None = None,
        *,
        columns: Seq[str] | None = None,
        sequences: Seq[str] | None = None,
        order: bool = False,
        limit: int | None = None,
    ) -> Iterator[RecordBatch]:
        """Like `find`, but returns streaming RecordBatches: consume results larger
        than RAM without materializing them all."""
        ctx = self._ctx(topic)
        df = ctx.sql(self._build_sql(topic, where, columns, sequences, order, limit))
        for batch in df.execute_stream():
            yield batch.to_pyarrow()

    def catalog(self, topic: str, where: str) -> pa.Table:
        """Which sequences satisfy a physical condition, with count and time window.
        A GROUP BY on the partition column."""
        ctx = self._ctx(topic)
        sql = f"""
            SELECT {PART_COL} AS sequence,
                   COUNT(*)          AS matches,
                   MIN({TIMESTAMP})  AS first_ns,
                   MAX({TIMESTAMP})  AS last_ns
            FROM "{topic}"
            WHERE ({_quote_fields(where)})
            GROUP BY {PART_COL}
            ORDER BY matches DESC
        """
        return ctx.sql(sql).to_arrow_table()

    def sql(self, topic: str, query: str) -> pa.Table:
        """Full SQL. The table is named like the topic; `sequence` is a column."""
        return self._ctx(topic).sql(_quote_fields(query)).to_arrow_table()

    # --- ML SYNCHRONIZATION ----------------------------------------------

    def align(
        self,
        sequence: str,
        topics: Seq[str],
        hz: float,
        *,
        columns: dict[str, Seq[str]] | None = None,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> pa.Table:
        """Align several sensors of one sequence onto a fixed `hz` grid
        (zero-order hold). See `tessera.sync.align`."""
        from .sync import align as _align

        src = {t: self._read_sequence(t, sequence) for t in topics}
        return _align(src, hz, columns=columns, start_ns=start_ns, end_ns=end_ns)

    def _read_sequence(self, topic: str, sequence: str) -> pa.Table:
        """Read all part files of one (topic, sequence) as a single table."""
        part_dir = f"{self._base}/{topic}/{PART_COL}={sequence}"
        ds = pads.dataset(part_dir, filesystem=self._fs, format="parquet")
        return ds.to_table()

    # --- CATALOG ----------------------------------------------------------

    def topics(self) -> list[str]:
        sel = pafs.FileSelector(self._base, allow_not_found=True)
        return sorted(
            Path(f.path).name for f in self._fs.get_file_info(sel)
            if f.type == pafs.FileType.Directory
        )

    def sequences(self, topic: str) -> list[str]:
        sel = pafs.FileSelector(f"{self._base}/{topic}", allow_not_found=True)
        out = []
        for f in self._fs.get_file_info(sel):
            name = Path(f.path).name
            if f.type == pafs.FileType.Directory and name.startswith(f"{PART_COL}="):
                out.append(name.split("=", 1)[1])
        return sorted(out)
