# 🌽💪 Cornflex 🌽💪

A lightweight SFTP reader built on top of `paramiko` and `polars`. Connects to remote servers and pulls files down as strings, Polars DataFrames, or raw text.

---

## Installation

```bash
pip install paramiko polars chardet
```

Then drop `cornflex/` into your project.

---

## Usage

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

## Methods

### `get_files(remote_path, file_pattern)`

Lists files in a remote directory. Supports glob-style patterns.

```python
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

## Notes

- `get_files()` handles connect/close internally.
- For all other methods, call `connect()` before and `close()` after.
- Either `password` or `pem_file` must be provided — not both, not neither.
- `get_csv_file` assumes UTF-8. Use `file_to_string` for other encodings.

---

## Dependencies

| Package | Purpose |
|---|---|
| `paramiko` | SSH/SFTP connection |
| `polars` | DataFrame output |
| `chardet` | Encoding detection |