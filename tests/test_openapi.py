import pytest

from cornflex.openapi import (
    describe_endpoint,
    describe_openapi,
    describe_schema,
    find_endpoints,
    list_endpoints,
)

SPEC = {
    "openapi": "3.0.1",
    "info": {"title": "Demo", "version": "1.0"},
    "servers": [{"url": "https://x/api"}],
    "paths": {
        "/v1/persons": {
            "parameters": [
                {"name": "X-Trace", "in": "header", "schema": {"type": "string"}}
            ],
            "post": {
                "tags": ["person"],
                "summary": "List persons",
                "operationId": "listPersons",
                "parameters": [{"$ref": "#/components/parameters/Page"}],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Req"}
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {"$ref": "#/components/schemas/Person"},
                                }
                            }
                        },
                    },
                    "404": {"description": "not found"},
                },
            },
        },
        "/v1/ping": {"get": {"responses": {"200": {"description": "pong"}}}},
    },
    "components": {
        "parameters": {
            "Page": {
                "name": "page",
                "in": "query",
                "schema": {"type": "integer", "minimum": 0},
            }
        },
        "schemas": {
            "Req": {
                "type": "object",
                "required": ["ids"],
                "properties": {
                    "ids": {
                        "type": "array",
                        "maxItems": 100,
                        "items": {"type": "string", "format": "uuid"},
                    }
                },
            },
            "Base": {
                "type": "object",
                "properties": {"id": {"type": "integer", "format": "int64"}},
            },
            "Person": {
                "allOf": [
                    {"$ref": "#/components/schemas/Base"},
                    {
                        "type": "object",
                        "required": ["name"],
                        "properties": {
                            "name": {"type": "string"},
                            "status": {"$ref": "#/components/schemas/Status"},
                            "boss": {"$ref": "#/components/schemas/Person"},
                            "nick": {"type": ["string", "null"]},
                            "tags": {
                                "type": "object",
                                "additionalProperties": {"type": "string"},
                            },
                            "gone": {"$ref": "#/components/schemas/Missing"},
                        },
                    },
                ]
            },
            "Status": {"type": "string", "enum": ["A", "B"]},
        },
    },
}


def test_list_endpoints_returns_all_operations() -> None:
    endpoints = list_endpoints(SPEC)
    assert [(e["method"], e["path"]) for e in endpoints] == [
        ("POST", "/v1/persons"),
        ("GET", "/v1/ping"),
    ]
    assert endpoints[0]["operation_id"] == "listPersons"
    assert endpoints[1]["tags"] == ["(untagged)"]


def test_describe_openapi_groups_by_tag_and_lists_schemas() -> None:
    out = describe_openapi(SPEC)
    assert out.startswith("Demo  version 1.0  (OpenAPI 3.0.1)")
    assert "server: https://x/api" in out
    assert "[person]" in out and "[(untagged)]" in out
    assert "4 schemas in components: Base, Person, Req, Status" in out


def test_find_endpoints_matches_case_insensitively() -> None:
    assert "/v1/persons" in find_endpoints(SPEC, "LISTPERSONS")
    assert find_endpoints(SPEC, "zzz") == "no endpoints match 'zzz'"


def test_describe_endpoint_shows_parameters_body_and_responses() -> None:
    out = describe_endpoint(SPEC, "/v1/persons", "post")
    assert "X-Trace" in out and "in=header" in out  # path-level parameter
    assert "page" in out and "in=query" in out  # $ref parameter
    assert "Request body  application/json, required  -> Req" in out
    assert "ids*" in out and "[max 100 items]" in out
    assert "200  application/json  -> array<Person>" in out
    assert "404  not found" in out


def test_describe_schema_flattens_allof_and_marks_required() -> None:
    out = describe_schema(SPEC, "Person")
    assert "id " in out and "integer(int64)" in out  # inherited from Base
    assert "name*" in out
    assert "Status (string enum[A, B])" in out
    assert "nick" in out and "string | null" in out
    assert "map<string, string>" in out


def test_recursive_schema_does_not_loop() -> None:
    assert "(recursive: Person, shown above)" in describe_schema(SPEC, "Person")


def test_dangling_ref_is_shown_as_is() -> None:
    assert "Missing" in describe_schema(SPEC, "Person")


def test_describe_endpoint_unknown_path_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No endpoint '/nope'"):
        describe_endpoint(SPEC, "/nope")


def test_describe_endpoint_unknown_method_lists_available() -> None:
    with pytest.raises(ValueError, match="available: GET"):
        describe_endpoint(SPEC, "/v1/ping", "post")


def test_describe_schema_unknown_name_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No schema 'Nope'"):
        describe_schema(SPEC, "Nope")


def test_find_api_docs_spec_feeds_describe_openapi() -> None:
    from unittest.mock import MagicMock

    from cornflex import find_api_docs

    reader = MagicMock(base_url="https://x/api")
    reader.get_json.return_value = SPEC
    url, spec = find_api_docs(reader)
    assert url == "https://x/openapi.json"
    assert describe_openapi(spec).startswith("Demo")
