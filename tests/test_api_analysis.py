from unittest.mock import MagicMock

import pytest
import requests

from cornflex.api_analysis import describe_api, find_api_docs, infer_schema

RECORDS = [
    {"id": 1, "name": "a", "created": "2024-01-01T10:00:00Z", "note": None},
    {"id": 2, "name": "b", "created": "2024-01-02T10:00:00Z", "note": "x"},
]


def test_infer_schema_types_and_nullability() -> None:
    schema = infer_schema(RECORDS)
    assert schema.record_count == 2
    assert schema.fields["id"].sql_type == "BIGINT"
    assert schema.fields["created"].sql_type == "TIMESTAMPTZ"
    assert not schema.is_nullable("id")
    assert schema.is_nullable("note")


def test_infer_schema_key_and_timestamp_candidates() -> None:
    schema = infer_schema(RECORDS)
    assert schema.key_candidates()[0] == "id"
    assert schema.timestamp_columns() == ["created"]


def test_infer_schema_nested_array_becomes_child_table() -> None:
    schema = infer_schema([{"id": 1, "lines": [{"sku": "a"}]}, {"id": 2, "lines": []}])
    assert schema.child_tables() == ["lines"]


def test_ddl_lists_columns() -> None:
    ddl = infer_schema(RECORDS).ddl("orders")
    assert ddl.startswith("CREATE TABLE orders (")
    assert "id" in ddl and "BIGINT NOT NULL" in ddl


def test_describe_api_prints_report_and_returns_schema(capsys: pytest.CaptureFixture) -> None:
    response = MagicMock(status_code=200, url="https://api/x", content=b"{}")
    response.headers = {"Content-Type": "application/json", "X-Total-Count": "2"}
    response.json.return_value = {"data": RECORDS, "next": None}
    reader = MagicMock()
    reader.get_response.return_value = response
    reader.get_records.return_value = RECORDS

    schema = describe_api(reader, "orders", table="orders")

    out = capsys.readouterr().out
    assert schema.record_count == 2
    assert "data" in out and "<- using" in out
    assert "X-Total-Count" in out
    assert "CREATE TABLE orders" in out
    assert reader.get_records.call_args.args[2] == "data"
    reader.get_response.assert_called_once()
    assert reader.get_records.call_args.kwargs["first_response"] is response


def test_find_api_docs_returns_spec_and_url() -> None:
    reader = MagicMock(base_url="https://api.example.com/v1")
    spec = {"openapi": "3.0.0", "paths": {}}
    reader.get_json.side_effect = [RuntimeError("404"), spec]

    assert find_api_docs(reader) == ("https://api.example.com/swagger.json", spec)


def test_find_api_docs_returns_none_when_nothing_found() -> None:
    reader = MagicMock(base_url="https://api.example.com/v1")
    reader.get_json.side_effect = [
        RuntimeError("404"),
        ValueError("not json"),
        requests.ConnectionError(),
        {"no": "paths"},
    ] + [RuntimeError("404")] * 10

    assert find_api_docs(reader) is None
