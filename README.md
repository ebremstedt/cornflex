# 🌽💪 Cornflex 🌽💪

A lightweight reader for SFTP servers and JSON HTTPS APIs, built on `paramiko`, `requests` and `polars`. Pulls files and API records down as strings, Polars DataFrames, or raw text.

---

## Installation

```bash
pip install cornflex
```

---

## SFTP

```python
from cornflex import SFTPReader
```

### Connect with password

```python
reader = SFTPReader(
    hostname="sftp.example.com",
    username="myuser",
    password="mypassword",
)
```

### Connect with PEM key

```python
reader = SFTPReader(
    hostname="sftp.example.com",
    username="myuser",
    pem_file="/path/to/key.pem",
)
```

---

### Methods

### `get_files(remote_path, file_pattern)`

Lists files in a remote directory. Supports glob-style patterns.

```python
with reader:
    reader.get_files(remote_path="/data", file_pattern="*.csv")
# ["orders_2024.csv", "users_2024.csv"]
```

---

### `get_csv_file(file_name, remote_path, column_names)`

Fetches a CSV and returns it as a Polars DataFrame.

```python
reader.connect()
df = reader.get_csv_file("orders.csv", remote_path="/data")

# Override column names (useful when file has no header)
df = reader.get_csv_file("orders.csv", remote_path="/data", column_names=["id", "amount", "date"])
reader.close()
```

---

### `get_xml_file_to_string(file_name, remote_path)`

Fetches an XML file and returns it as a string.

```python
reader.connect()
xml = reader.get_xml_file_to_string("feed.xml", remote_path="/exports")
reader.close()
```

---

### `file_to_string(file_name, remote_path, encoding)`

Fetches any file as a string. Auto-detects encoding via `chardet` if not specified.

```python
reader.connect()
content = reader.file_to_string("report.txt", remote_path="/reports")

# Force encoding
content = reader.file_to_string("report.txt", encoding="latin-1")
reader.close()
```

---

## API

```python
from cornflex import APIReader

with APIReader(
    base_url="https://api.example.com/v1",
    pfx_file="/path/to/client.pfx",   # optional client certificate
    pfx_password="secret",
    verify=True,                      # False skips TLS verification, or a CA bundle path
) as api:
    data = api.get_json("orders")
    records = api.get_records("orders", records_path="data.items", max_pages=5)
    df = api.get_dataframe("orders", max_pages=5)

    # POST a JSON body and get the parsed JSON back
    result = api.post_json("persons-list-request", json={"ids": batch})
```

- Errors (status 400 and above) include the first 300 characters of the response body. Set `include_body_in_errors=False` when responses may echo sensitive input; the exception then holds only `METHOD url returned status`.
- `get_records` / `get_dataframe` are GET-only; for POST endpoints use `post_json` and build the records yourself.
- `get_records` / `get_dataframe` auto-detect the largest array of objects unless `records_path` (dot-path, `"$"` for the root) is given.
- Pagination is followed up to `max_pages` via a `Link` header or a `next` / `links.next` field. Requests never leave the host of `base_url`, so the client certificate is not sent elsewhere.

### Analysing an API

```python
from cornflex import APIReader, describe_api, find_api_docs, infer_schema

with APIReader(base_url="https://api.example.com/v1") as api:
    describe_api(api, "orders", table="orders")   # prints structure, pagination hints, schema and CREATE TABLE
    find_api_docs(api)                            # (url, spec) of an OpenAPI/Swagger doc, or None
    schema = infer_schema(api.get_records("orders"))
```

### Reading an OpenAPI spec

`find_api_docs` returns the spec; the `describe_*` helpers turn it into readable text (OpenAPI 3 only).

```python
from cornflex import (
    APIReader, find_api_docs, describe_openapi, find_endpoints, describe_endpoint, describe_schema,
)

with APIReader(base_url="https://api.example.com") as api:
    url, spec = find_api_docs(api)      # or: spec = api.get_json("/v3/api-docs")

print(describe_openapi(spec))                              # title, servers, endpoints grouped by tag
print(find_endpoints(spec, "person"))                      # search path / summary / operationId
print(describe_endpoint(spec, "/v1/persons", "post"))      # parameters, request body, responses
print(describe_schema(spec, "Person"))                     # one model as a field tree (* = required)
```

`list_endpoints(spec)` returns the same endpoints as a list of dicts. `describe_endpoint` and `describe_schema` raise `ValueError` for an unknown path, method or schema name.

---

## Notes

- All methods need an open connection: use `with reader:` or call `connect()` and `close()`.
- Either `password` or `pem_file` must be provided — not both, not neither.
- `get_csv_file` assumes UTF-8. Use `file_to_string` for other encodings.

---

## Dependencies

| Package | Purpose |
|---|---|
| `paramiko` | SSH/SFTP connection |
| `requests` | HTTPS API calls |
| `cryptography` | Reading `.pfx` client certificates |
| `polars` | DataFrame output |
| `chardet` | Encoding detection |