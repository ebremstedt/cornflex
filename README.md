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
```

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