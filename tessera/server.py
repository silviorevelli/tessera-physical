"""
Server — expose a `Lake` to multiple clients over Apache Arrow Flight.

A network service on top of a `Lake`, using Arrow Flight (gRPC + Arrow), the standard
protocol for serving Arrow data. It adds, relative to the in-process library:

  * Multi-client access: clients read and write concurrently (Flight uses a gRPC
    thread pool); writes are append-only part files, so they do not collide.
  * Authentication: optional API keys checked on every call (Bearer token in the
    Flight `authorization` header).
  * Remote access: clients talk to one shared lake (local disk or S3/GCS/Azure).

Protocol mapping (standard Flight verbs):
  * do_get    -> queries: find / catalog / align / sql  (streams Arrow back)
  * do_put    -> writes:  stream an Arrow table into (topic, sequence)
  * do_action -> metadata: topics, sequences, ping

Start it with:  TesseraServer(Lake("./datalake")).serve()
"""

from __future__ import annotations

import json
from typing import Iterable

import pyarrow as pa
import pyarrow.flight as fl

from .lake import Lake


class _ApiKeyMiddlewareFactory(fl.ServerMiddlewareFactory):
    """Checks a Bearer API key on every call. If no keys are configured, access
    is open (single-tenant/dev mode)."""

    def __init__(self, api_keys: set[str] | None):
        self._keys = set(api_keys) if api_keys else None

    def start_call(self, info, headers):
        if self._keys is None:
            return None
        values = headers.get("authorization") or headers.get("Authorization")
        token = values[0].split(" ", 1)[-1].strip() if values else None
        if token not in self._keys:
            raise fl.FlightUnauthenticatedError("invalid or missing API key")
        return None


class TesseraServer(fl.FlightServerBase):
    """Serves a `Lake` over Arrow Flight to many clients.

    Args:
        lake: the Lake (local or object-store backed) to expose.
        location: gRPC bind address, e.g. "grpc://0.0.0.0:8815".
        api_keys: optional set of valid API keys; if None, access is open.
    """

    def __init__(
        self,
        lake: Lake,
        location: str = "grpc://0.0.0.0:8815",
        *,
        api_keys: Iterable[str] | None = None,
    ):
        middleware = {"auth": _ApiKeyMiddlewareFactory(set(api_keys) if api_keys else None)}
        super().__init__(location, middleware=middleware)
        self.lake = lake
        self._location = location

    # --- queries -> streamed Arrow ---------------------------------------

    def do_get(self, context, ticket) -> fl.RecordBatchStream:
        req = json.loads(ticket.ticket.decode())
        table = self._run_query(req)
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        return fl.RecordBatchStream(reader)

    def _run_query(self, req: dict) -> pa.Table:
        op = req.get("op")
        if op == "find":
            return self.lake.find(
                req["topic"], req.get("where"),
                columns=req.get("columns"), sequences=req.get("sequences"),
                order=req.get("order", True), limit=req.get("limit"),
            )
        if op == "catalog":
            return self.lake.catalog(req["topic"], req["where"])
        if op == "align":
            return self.lake.align(
                req["sequence"], req["topics"], req["hz"],
                columns=req.get("columns"),
                start_ns=req.get("start_ns"), end_ns=req.get("end_ns"),
            )
        if op == "sql":
            return self.lake.sql(req["topic"], req["query"])
        raise fl.FlightServerError(f"unknown query op: {op!r}")

    # --- writes -> stream Arrow in ---------------------------------------

    def do_put(self, context, descriptor, reader, writer) -> None:
        req = json.loads(descriptor.command.decode())
        table = reader.read_all()
        self.lake.write_table(req["sequence"], req["topic"], table)

    # --- metadata / admin ------------------------------------------------

    def do_action(self, context, action):
        body = action.body.to_pybytes() if action.body else b"{}"
        req = json.loads(body.decode() or "{}")
        if action.type == "topics":
            res = self.lake.topics()
        elif action.type == "sequences":
            res = self.lake.sequences(req["topic"])
        elif action.type == "ping":
            res = "pong"
        else:
            raise fl.FlightServerError(f"unknown action: {action.type!r}")
        yield fl.Result(json.dumps(res).encode())

    def list_actions(self, context):
        return [
            ("topics", "list topics"),
            ("sequences", "list sequences of a topic"),
            ("ping", "health check"),
        ]
