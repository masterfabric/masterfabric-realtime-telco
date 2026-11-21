"""Avro encoding in the Confluent wire format, with schemas held in a schema registry.

Wire format of every Kafka value: one magic byte 0x00, the 4-byte big-endian schema id assigned by
the registry, then the Avro binary body (no embedded schema). Consumers fetch the writer schema by id.
The registry is reached through its Confluent-compatible REST API (Apicurio exposes it under
/apis/ccompat/v7), so the same code works against Confluent Schema Registry or Karapace.

Only the standard library and fastavro are used, to keep the wire format visible instead of hidden
behind a client library.
"""
from __future__ import annotations

import io
import json
import struct
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

import fastavro

SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
MAGIC = 0
CT = "application/vnd.schemaregistry.v1+json"


def load_schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / f"{name}.avsc").read_text())


class Registry:
    """Minimal client for the Confluent-compatible schema registry API."""

    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def _call(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method,
                                     headers={"Content-Type": CT, "Accept": CT})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"message": raw.decode(errors="replace")}

    def set_compatibility(self, subject: str, level: str) -> tuple[int, dict]:
        return self._call("PUT", f"/config/{subject}", {"compatibility": level})

    def check(self, subject: str, schema: dict) -> tuple[int, dict]:
        return self._call("POST", f"/compatibility/subjects/{subject}/versions/latest?verbose=true",
                          {"schema": json.dumps(schema)})

    def register(self, subject: str, schema: dict) -> tuple[int, dict]:
        return self._call("POST", f"/subjects/{subject}/versions", {"schema": json.dumps(schema)})

    def versions(self, subject: str) -> tuple[int, dict]:
        return self._call("GET", f"/subjects/{subject}/versions")

    def schema_by_id(self, schema_id: int) -> dict:
        _, body = self._call("GET", f"/schemas/ids/{schema_id}")
        return json.loads(body["schema"])


class AvroEncoder:
    """Encodes producer records (dicts with an ISO event_ts) for one registered schema."""

    def __init__(self, schema: dict, schema_id: int):
        self.parsed = fastavro.parse_schema(schema)
        self.header = struct.pack(">bI", MAGIC, schema_id)
        self.fields = [f["name"] for f in schema["fields"]]

    def encode(self, rec: dict) -> bytes:
        r = {k: rec.get(k) for k in self.fields}
        r["event_ts"] = datetime.fromisoformat(rec["event_ts"].replace("Z", "+00:00"))
        buf = io.BytesIO()
        buf.write(self.header)
        fastavro.schemaless_writer(buf, self.parsed, r)
        return buf.getvalue()


def decode(value: bytes, schemas: dict[int, dict]) -> tuple[int, dict]:
    """Inverse of AvroEncoder.encode, given {schema_id: parsed writer schema}."""
    magic, schema_id = struct.unpack(">bI", value[:5])
    if magic != MAGIC:
        raise ValueError("not Confluent wire format")
    return schema_id, fastavro.schemaless_reader(io.BytesIO(value[5:]), schemas[schema_id])
