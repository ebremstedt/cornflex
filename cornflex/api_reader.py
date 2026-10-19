import atexit
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import polars as pl
import requests
import urllib3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12


@dataclass
class APIReader:
    """
    Reads JSON from an HTTPS API, optionally authenticated with a .pfx/.p12 client certificate.

    Equivalent of: curl -k --cert-type P12 --cert cert.pfx:<password> https://<url>
    (verify=False is curl's -k).
    """

    base_url: str
    pfx_file: Optional[str] = None
    pfx_password: Optional[str] = None
    verify: bool | str = True  # False = skip TLS verification (curl -k), str = path to CA bundle
    headers: dict[str, str] | None = None
    timeout: int = 60

    def __post_init__(self) -> None:
        self._session: Optional[requests.Session] = None
        self._temp_files: list[str] = []
        self._atexit_registered = False

    def connect(self) -> None:
        session = requests.Session()

        if self.pfx_file:
            cert_path, key_path = self._pfx_to_pem()
            session.cert = (cert_path, key_path)

        if self.verify is False:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session.headers.update({"Accept": "application/json", **(self.headers or {})})
        self._session = session

    def close(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
        for path in self._temp_files:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
        self._temp_files = []

    def __enter__(self) -> "APIReader":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def get_response(self, path: str = "", params: dict | None = None) -> requests.Response:
        session = self._require_session()
        url = urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/")) if not path.startswith("http") else path

        # never send the client certificate to a host the user didn't point us at
        if urlparse(url).netloc != urlparse(self.base_url).netloc:
            raise ValueError(f"Refusing to call {url}: different host than base_url")

        response = session.get(url, params=params, timeout=self.timeout, verify=self.verify)
        if response.status_code >= 400:
            raise RuntimeError(f"GET {response.url} returned {response.status_code}: {response.text[:300]}")
        return response

    def get_json(self, path: str = "", params: dict | None = None) -> Any:
        return self.get_response(path, params).json()

    def get_records(
        self,
        path: str = "",
        params: dict | None = None,
        records_path: str | None = None,
        max_pages: int = 1,
    ) -> list[dict]:
        response = self.get_response(path, params)
        body = response.json()

        if records_path is None:
            arrays = find_record_arrays(body)
            records_path = max(arrays, key=lambda a: len(a[1]))[0] if arrays else "$"

        records = extract_records(body, records_path)

        for _ in range(max_pages - 1):
            next_url = _find_next_url(body, response)
            if not next_url:
                break
            response = self.get_response(urljoin(response.url, next_url))
            body = response.json()
            records.extend(extract_records(body, records_path))

        return records

    def get_dataframe(
        self,
        path: str = "",
        params: dict | None = None,
        records_path: str | None = None,
        max_pages: int = 1,
    ) -> pl.DataFrame:
        records = self.get_records(path, params, records_path, max_pages)
        return pl.from_dicts(records, infer_schema_length=None)

    def _require_session(self) -> requests.Session:
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._session

    def _pfx_to_pem(self) -> tuple[str, str]:
        """requests can't read .pfx, so write temporary PEM files (removed on close/exit)."""
        data = Path(self.pfx_file).read_bytes()
        password = self.pfx_password.encode() if self.pfx_password else None
        key, cert, extra = pkcs12.load_key_and_certificates(data, password)
        if key is None or cert is None:
            raise ValueError(f"{self.pfx_file} must contain both a certificate and a private key")

        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        for extra_cert in extra or []:
            cert_pem += extra_cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )

        paths = []
        for suffix, content in (("-cert.pem", cert_pem), ("-key.pem", key_pem)):
            fd, path = tempfile.mkstemp(suffix=suffix)  # created with 0600 permissions
            with os.fdopen(fd, "wb") as f:
                f.write(content)
            paths.append(path)
            self._temp_files.append(path)

        if not self._atexit_registered:
            atexit.register(self.close)  # safety net if the caller forgets close()
            self._atexit_registered = True
        return paths[0], paths[1]


def find_record_arrays(node: Any, path: str = "") -> list[tuple[str, list]]:
    """Return every (path, list_of_dicts) in a JSON payload. A top-level array has path '$'."""
    found: list[tuple[str, list]] = []
    if isinstance(node, list):
        if node and all(isinstance(x, dict) for x in node):
            found.append((path or "$", node))
        for i, item in enumerate(node[:3]):
            found += find_record_arrays(item, f"{path}[{i}]")
    elif isinstance(node, dict):
        for key, value in node.items():
            found += find_record_arrays(value, f"{path}.{key}" if path else key)
    return found


def extract_records(body: Any, records_path: str) -> list[dict]:
    """Follow a dot-path ('data.items', or '$' for the root) and return it as a list of records."""
    node = body
    if records_path != "$":
        for part in records_path.split("."):
            node = node[part]
    if isinstance(node, dict):
        return [node]
    return list(node)


def _find_next_url(body: Any, response: requests.Response) -> str | None:
    if response.links.get("next", {}).get("url"):
        return response.links["next"]["url"]
    if isinstance(body, dict):
        for key in ("next", "next_url", "nextLink", "@odata.nextLink", "next_page"):
            if isinstance(body.get(key), str):
                return body[key]
        links = body.get("links") or body.get("_links")
        if isinstance(links, dict):
            nxt = links.get("next")
            if isinstance(nxt, str):
                return nxt
            if isinstance(nxt, dict) and nxt.get("href"):
                return nxt["href"]
    return None
