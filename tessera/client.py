"""
Client — access a Tessera server over Arrow Flight.

Mirrors the `Lake` API (find / catalog / align / write / topics / sequences) over the
network, so multiple clients can share one lake:

    from tessera import TesseraClient, IMU, Vec3

    client = TesseraClient("grpc://server:8815", api_key="...")
    client.write("run01", "imu", [IMU(t, Vec3(...), Vec3(...)) for t in ...])
    client.find("imu", "acceleration.x > 5")
"""

from __future__ import annotations

import json
from typing import Iterable, Sequence as Seq

import pyarrow as pa
import pyarrow.flight as fl

from .ontology import Sensor

TIMESTAMP = "timestamp_ns"
PART_COL = "sequence"


class TesseraClient:
    def __init__(self, location: str = "grpc://127.0.0.1:8815", *, api_key: str | None = None):
        self._client = fl.FlightClient(location)
        self._opts = None
        if api_key:
            self._opts = fl.FlightCallOptions(
                headers=[(b"authorization", f"Bearer {api_key}".encode())]
            )

    # --- queries ----------------------------------------------------------

    def _get(self, req: dict) -> pa.Table:
        ticket = fl.Ticket(json.dumps(req).encode())
        return self._client.do_get(ticket, self._opts).read_all()

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
        return self._get({"op": "find", "topic": topic, "where": where,
                          "columns": list(columns) if columns else None,
                          "sequences": list(sequences) if sequences else None,
                          "order": order, "limit": limit})

    def catalog(self, topic: str, where: str) -> pa.Table:
        return self._get({"op": "catalog", "topic": topic, "where": where})

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
        return self._get({"op": "align", "sequence": sequence, "topics": list(topics),
                          "hz": hz, "columns": columns,
                          "start_ns": start_ns, "end_ns": end_ns})

    def sql(self, topic: str, query: str) -> pa.Table:
        return self._get({"op": "sql", "topic": topic, "query": query})

    # --- writes -----------------------------------------------------------

    def write(self, sequence: str, topic: str, records: Iterable[Sensor | dict]) -> None:
        rows = [r.row() if isinstance(r, Sensor) else dict(r) for r in records]
        if not rows:
            raise ValueError("no records to write")
        for r in rows:
            r.pop(PART_COL, None)
        self.write_table(sequence, topic, pa.Table.from_pylist(rows))

    def write_table(self, sequence: str, topic: str, table: pa.Table) -> None:
        descriptor = fl.FlightDescriptor.for_command(
            json.dumps({"op": "write", "sequence": sequence, "topic": topic}).encode()
        )
        writer, _ = self._client.do_put(descriptor, table.schema, self._opts)
        writer.write_table(table)
        writer.close()

    # --- metadata ---------------------------------------------------------

    def _action(self, typ: str, payload: dict | None = None):
        action = fl.Action(typ, json.dumps(payload or {}).encode())
        results = list(self._client.do_action(action, self._opts))
        return json.loads(results[0].body.to_pybytes()) if results else None

    def topics(self) -> list[str]:
        return self._action("topics")

    def sequences(self, topic: str) -> list[str]:
        return self._action("sequences", {"topic": topic})

    def ping(self) -> str:
        return self._action("ping")
