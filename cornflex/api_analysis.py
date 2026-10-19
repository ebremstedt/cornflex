"""Explore an API and infer a flat schema from its JSON records so you can sketch an ETL pipeline."""

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import requests

from cornflex.api_reader import APIReader, find_record_arrays

_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")
_NUMERIC_STR_RE = re.compile(r"^-?\d+(\.\d+)?$")

_MAX_DISTINCT = 100_000  # stop tracking distinct values after this many


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        if _DATETIME_RE.match(value):
            return "datetime-string"
        if _DATE_RE.match(value):
            return "date-string"
        if _UUID_RE.match(value):
            return "uuid-string"
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


@dataclass
class FieldInfo:
    path: str
    types: Counter = field(default_factory=Counter)
    seen: int = 0
    nulls: int = 0
    examples: list = field(default_factory=list)
    distinct: set = field(default_factory=set)
    max_len: int = 0
    numeric_strings: int = 0

    def add(self, value: Any) -> None:
        self.types[json_type(value)] += 1
        self.seen += 1
        if value is None:
            self.nulls += 1
            return
        if isinstance(value, (str, int, float, bool)):
            if len(self.distinct) < _MAX_DISTINCT:
                self.distinct.add(value)
            if len(self.examples) < 3 and value not in self.examples:
                self.examples.append(value)
        if isinstance(value, str):
            self.max_len = max(self.max_len, len(value))
            if _NUMERIC_STR_RE.match(value):
                self.numeric_strings += 1

    @property
    def sql_type(self) -> str:
        types = set(self.types) - {"null"}
        if not types:
            return "TEXT  -- always null in sample"
        if types <= {"integer"}:
            return "BIGINT"
        if types <= {"integer", "number"}:
            return "DOUBLE PRECISION"
        if types <= {"boolean"}:
            return "BOOLEAN"
        if types <= {"datetime-string"}:
            return "TIMESTAMPTZ"
        if types <= {"date-string"}:
            return "DATE"
        if types <= {"date-string", "datetime-string"}:
            return "TIMESTAMP"
        if types <= {"uuid-string"}:
            return "UUID"
        if types <= {"string", "uuid-string", "date-string", "datetime-string"}:
            if self.max_len > 1000:
                return "TEXT"
            if self.numeric_strings == self.seen - self.nulls:
                return f"VARCHAR({max(self.max_len * 2, 20)})  -- numeric-looking string, cast?"
            return f"VARCHAR({max(self.max_len * 2, 50)})"
        if types <= {"object", "array"}:
            return "JSONB"
        return "TEXT  -- mixed types: " + ", ".join(sorted(types))


@dataclass
class Schema:
    record_count: int
    fields: dict[str, FieldInfo]

    def is_nullable(self, path: str) -> bool:
        info = self.fields[path]
        if "[]" in path:
            return True
        return info.nulls > 0 or info.seen < self.record_count

    def key_candidates(self) -> list[str]:
        """Top-level-ish fields that are non-null and unique across all records. Id-like names first."""
        n = self.record_count
        if n < 2 or n > _MAX_DISTINCT:
            return []
        idlike, other = [], []
        for path, info in self.fields.items():
            if "[]" in path or info.seen != n or info.nulls or len(info.distinct) != n:
                continue
            name = path.lower().split(".")[-1]
            if name in ("id", "uuid", "guid") or name.endswith(("id", "key")):
                idlike.append(path)
            else:
                other.append(path)
        return idlike + other

    def timestamp_columns(self) -> list[str]:
        return [
            p for p, i in self.fields.items()
            if "[]" not in p and {"datetime-string", "date-string"} & set(i.types)
        ]

    def child_tables(self) -> list[str]:
        """Arrays of objects - candidates for their own table."""
        return sorted(
            p for p, i in self.fields.items()
            if "array" in i.types and any(other.startswith(p + "[].") for other in self.fields)
        )

    def mixed_type_fields(self) -> list[str]:
        return [p for p, i in self.fields.items() if len(set(i.types) - {"null"}) > 1]

    def columns(self) -> list[str]:
        """Paths that would become columns of the parent table (leaves, not inside arrays)."""
        return sorted(
            p for p in self.fields
            if "[]" not in p and not any(o.startswith(p + ".") for o in self.fields)
        )

    def ddl(self, table: str = "my_table") -> str:
        lines = []
        for path in self.columns():
            column = re.sub(r"\W+", "_", path).strip("_").lower()
            not_null = "" if self.is_nullable(path) else " NOT NULL"
            sql = self.fields[path].sql_type
            if "  --" in sql:
                sql, comment = sql.split("  --", 1)
                lines.append(f"    {column:<35} {sql}{not_null}  --{comment}")
            else:
                lines.append(f"    {column:<35} {sql}{not_null}")
        return f"CREATE TABLE {table} (\n" + ",\n".join(lines) + "\n);"

    def to_dict(self) -> dict:
        return {
            "record_count": self.record_count,
            "fields": {
                p: {
                    "types": dict(i.types),
                    "nullable": self.is_nullable(p),
                    "distinct_sampled": len(i.distinct),
                    "examples": i.examples,
                    "suggested_sql_type": i.sql_type.split("  --")[0],
                }
                for p, i in self.fields.items()
            },
        }

    def report(self) -> str:
        def short(v: Any, n: int = 28) -> str:
            s = v if isinstance(v, str) else repr(v)
            return s if len(s) <= n else s[: n - 1] + "…"

        n = self.record_count
        header = f"{'path':<42} {'type(s)':<24} {'null%':>6} {'distinct':>9}  examples"
        out = [f"Schema ({n} records analysed)", "", header, "-" * (len(header) + 20)]
        for path in sorted(self.fields, key=lambda p: (p.count("."), p)):
            i = self.fields[path]
            types = "/".join(t for t, _ in i.types.most_common())
            if "[]" in path:
                null_pct = "-"
            else:
                null_pct = f"{100 * (i.nulls + max(n - i.seen, 0)) / n:.0f}%"
            distinct = len(i.distinct) or "-"
            examples = ", ".join(short(e) for e in i.examples)
            out.append(f"{path:<42} {types:<24} {null_pct:>6} {distinct!s:>9}  {examples}")

        out += [
            "",
            "ETL hints",
            f"  Primary-key candidates : {self.key_candidates() or 'none (composite key / hash?)'}",
            f"  Incremental columns    : {self.timestamp_columns() or 'none (full loads only?)'}",
            f"  Child-table candidates : {self.child_tables() or 'none'}",
            f"  Mixed-type fields      : {self.mixed_type_fields() or 'none'}",
        ]
        return "\n".join(out)


def _field(fields: dict[str, FieldInfo], path: str) -> FieldInfo:
    if path not in fields:
        fields[path] = FieldInfo(path)
    return fields[path]


def _walk(value: Any, path: str, fields: dict[str, FieldInfo]) -> None:
    _field(fields, path).add(value)
    if isinstance(value, dict):
        for key, child in value.items():
            _walk(child, f"{path}.{key}", fields)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                for key, child in item.items():
                    _walk(child, f"{path}[].{key}", fields)
            elif item is not None:
                _field(fields, f"{path}[]").add(item)


def infer_schema(records: list[dict]) -> Schema:
    fields: dict[str, FieldInfo] = {}
    for record in records:
        for key, value in record.items():
            _walk(value, key, fields)
    return Schema(record_count=len(records), fields=fields)


def describe_api(
    reader: APIReader,
    path: str = "",
    params: dict | None = None,
    records_path: str | None = None,
    max_pages: int = 1,
    table: str = "my_table",
) -> Schema:
    """Fetch the endpoint and print what an ETL pipeline needs to know. Returns the Schema."""
    response = reader.get_response(path, params)
    body = response.json()

    arrays = find_record_arrays(body)
    if records_path is None:
        records_path = max(arrays, key=lambda a: len(a[1]))[0] if arrays else "$"

    print(f"GET {response.url} -> {response.status_code} "
          f"{response.headers.get('Content-Type', '?')} "
          f"({len(response.content)} bytes)")

    print("\nTop level:")
    if isinstance(body, dict):
        for key, value in body.items():
            size = f"({len(value)} items)" if isinstance(value, (list, dict)) else repr(value)[:60]
            print(f"  {key:<30} {type(value).__name__:<8} {size}")
    else:
        print(f"  {type(body).__name__} with {len(body)} items")

    print("\nRecord arrays found:")
    for p, items in arrays:
        if "[" in p:  # skip the same nested array repeated per record
            continue
        marker = "  <- using" if p == records_path else ""
        print(f"  {p:<40} {len(items)} records{marker}")

    print("\nPagination hints:")
    hints = _pagination_hints(body, response)
    print("\n".join(f"  {h}" for h in hints) if hints else "  none detected")

    schema = infer_schema(reader.get_records(path, params, records_path, max_pages))
    print()
    print(schema.report())
    print()
    print(schema.ddl(table))
    return schema


def find_api_docs(reader: APIReader) -> tuple[str, dict] | None:
    """Look for an OpenAPI/Swagger spec next to reader.base_url. Returns (url, spec) or None."""
    parsed = urlparse(reader.base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip("/")
    candidates = [
        "/openapi.json", "/swagger.json", "/v3/api-docs", "/v2/api-docs",
        "/api-docs", "/swagger/v1/swagger.json", "/api/openapi.json",
        f"{base_path}/openapi.json", f"{base_path}/swagger.json",
    ]
    for candidate in dict.fromkeys(candidates):
        try:
            spec = reader.get_json(root + candidate)
        except (RuntimeError, ValueError, requests.RequestException):
            continue
        if isinstance(spec, dict) and "paths" in spec:
            return root + candidate, spec
    return None


def _pagination_hints(body: Any, response: requests.Response) -> list[str]:
    pattern = re.compile(r"next|prev|page|cursor|offset|limit|total|count|links?|more|size|token", re.I)
    hints = []
    if isinstance(body, dict):
        for key, value in body.items():
            if pattern.search(key) and not (isinstance(value, list) and len(value) > 5):
                hints.append(f"body.{key} = {repr(value)[:80]}")
    for name, value in response.headers.items():
        if pattern.search(name):
            hints.append(f"header {name}: {value[:100]}")
    return hints
