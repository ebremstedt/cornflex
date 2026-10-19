import datetime
import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID
from polars.testing import assert_frame_equal

from cornflex.api_reader import APIReader, extract_records, find_record_arrays

BASE = "https://api.example.com/v1"


def make_response(
    body=None,
    url: str = f"{BASE}/orders",
    status: int = 200,
    links: dict | None = None,
) -> MagicMock:
    response = MagicMock()
    response.status_code = status
    response.url = url
    response.links = links or {}
    response.text = str(body)
    response.json.return_value = body
    return response


@pytest.fixture
def reader() -> APIReader:
    return APIReader(base_url=BASE)


@pytest.fixture
def session():
    with patch("cornflex.api_reader.requests.Session") as mock_cls:
        yield mock_cls.return_value


@pytest.fixture
def connected(reader: APIReader, session: MagicMock) -> APIReader:
    reader.connect()
    return reader


def _make_pfx(path, password: bytes | None, with_key: bool = True) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    encryption = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )
    data = pkcs12.serialize_key_and_certificates(
        b"test", key if with_key else None, cert, None, encryption
    )
    path.write_bytes(data)
    return str(path)


# --- connect / close ---


def test_connect_sets_default_and_custom_headers(session: MagicMock) -> None:
    APIReader(base_url=BASE, headers={"X-Api-Key": "abc"}).connect()
    session.headers.update.assert_called_once_with(
        {"Accept": "application/json", "X-Api-Key": "abc"}
    )


def test_connect_without_pfx_sets_no_cert(reader: APIReader, session: MagicMock) -> None:
    session.cert = None
    reader.connect()
    assert session.cert is None


def test_connect_with_pfx_writes_pem_files_and_close_removes_them(
    tmp_path, session: MagicMock
) -> None:
    pfx = _make_pfx(tmp_path / "client.pfx", b"secret")
    reader = APIReader(base_url=BASE, pfx_file=pfx, pfx_password="secret")
    reader.connect()

    cert_path, key_path = session.cert
    assert os.path.exists(cert_path) and os.path.exists(key_path)
    assert b"BEGIN CERTIFICATE" in open(cert_path, "rb").read()
    assert b"PRIVATE KEY" in open(key_path, "rb").read()

    reader.close()
    assert not os.path.exists(cert_path)
    assert not os.path.exists(key_path)


def test_connect_with_wrong_pfx_password_raises(tmp_path, session: MagicMock) -> None:
    pfx = _make_pfx(tmp_path / "client.pfx", b"secret")
    reader = APIReader(base_url=BASE, pfx_file=pfx, pfx_password="wrong")
    with pytest.raises(ValueError):
        reader.connect()


def test_connect_with_pfx_without_key_raises(tmp_path, session: MagicMock) -> None:
    pfx = _make_pfx(tmp_path / "client.pfx", None, with_key=False)
    reader = APIReader(base_url=BASE, pfx_file=pfx)
    with pytest.raises(ValueError, match="certificate and a private key"):
        reader.connect()


def test_close_without_connection_does_not_raise(reader: APIReader) -> None:
    reader.close()
    reader.close()


def test_close_closes_session(connected: APIReader, session: MagicMock) -> None:
    connected.close()
    session.close.assert_called_once()


def test_context_manager_connects_and_closes(session: MagicMock) -> None:
    with APIReader(base_url=BASE) as reader:
        assert reader._session is session
    session.close.assert_called_once()
    assert reader._session is None


# --- not connected ---


@pytest.mark.parametrize("method", ["get_response", "get_json", "post_json", "get_records", "get_dataframe"])
def test_methods_raise_when_not_connected(reader: APIReader, method: str) -> None:
    with pytest.raises(RuntimeError, match="Not connected"):
        getattr(reader, method)("orders")


# --- get_response ---


@pytest.mark.parametrize("path", ["orders", "/orders"])
def test_get_response_builds_url_from_base(
    connected: APIReader, session: MagicMock, path: str
) -> None:
    session.request.return_value = make_response([])
    connected.get_response(path)
    assert session.request.call_args.args[1] == f"{BASE}/orders"


def test_get_response_passes_params_timeout_and_verify(session: MagicMock) -> None:
    reader = APIReader(base_url=BASE, verify=False, timeout=5)
    reader.connect()
    session.request.return_value = make_response([])
    reader.get_response("orders", params={"page": 2})
    assert session.request.call_args.kwargs == {
        "params": {"page": 2},
        "json": None,
        "timeout": 5,
        "verify": False,
    }


def test_get_response_allows_absolute_url_on_same_host(
    connected: APIReader, session: MagicMock
) -> None:
    session.request.return_value = make_response([])
    connected.get_response(f"{BASE}/orders?page=2")
    assert session.request.call_args.args[1] == f"{BASE}/orders?page=2"


def test_get_response_refuses_other_host(connected: APIReader, session: MagicMock) -> None:
    with pytest.raises(ValueError, match="different host"):
        connected.get_response("https://evil.example.org/steal")
    session.request.assert_not_called()


def test_get_response_raises_on_http_error(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response("nope", status=404)
    with pytest.raises(RuntimeError, match="GET .* returned 404"):
        connected.get_response("orders")


def test_get_json_returns_parsed_body(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response({"a": 1})
    assert connected.get_json("orders") == {"a": 1}


# --- post_json ---


def test_post_json_sends_body_and_returns_parsed_response(
    connected: APIReader, session: MagicMock
) -> None:
    session.request.return_value = make_response({"persons": [{"id": 1}]})
    result = connected.post_json("/persons-list-request", json={"ids": ["a", "b"]})

    assert result == {"persons": [{"id": 1}]}
    assert session.request.call_args.args == ("POST", f"{BASE}/persons-list-request")
    assert session.request.call_args.kwargs["json"] == {"ids": ["a", "b"]}


def test_post_json_passes_params_timeout_and_verify(session: MagicMock) -> None:
    reader = APIReader(base_url=BASE, verify="/ca.pem", timeout=7)
    reader.connect()
    session.request.return_value = make_response({})
    reader.post_json("x", json={}, params={"v": 1})
    kwargs = session.request.call_args.kwargs
    assert (kwargs["params"], kwargs["timeout"], kwargs["verify"]) == ({"v": 1}, 7, "/ca.pem")


def test_post_json_raises_on_http_error(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response("bad", status=400)
    with pytest.raises(RuntimeError, match="POST .* returned 400"):
        connected.post_json("x", json={})


def test_post_json_refuses_other_host(connected: APIReader, session: MagicMock) -> None:
    with pytest.raises(ValueError, match="different host"):
        connected.post_json("https://evil.example.org/x", json={"secret": 1})
    session.request.assert_not_called()


def test_error_includes_body_by_default(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response("identity 19121212-1212", status=400)
    with pytest.raises(RuntimeError, match="19121212-1212"):
        connected.post_json("x", json={})


def test_error_omits_body_when_disabled(session: MagicMock) -> None:
    reader = APIReader(base_url=BASE, include_body_in_errors=False)
    reader.connect()
    session.request.return_value = make_response(
        "identity 19121212-1212", url=f"{BASE}/x", status=400
    )
    with pytest.raises(RuntimeError) as exc_info:
        reader.post_json("x", json={})
    assert str(exc_info.value) == f"POST {BASE}/x returned 400"


# --- get_records ---


def test_get_records_top_level_list(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response([{"id": 1}, {"id": 2}])
    assert connected.get_records("orders") == [{"id": 1}, {"id": 2}]


def test_get_records_autodetects_largest_array(connected: APIReader, session: MagicMock) -> None:
    body = {"meta": [{"x": 1}], "data": [{"id": 1}, {"id": 2}, {"id": 3}]}
    session.request.return_value = make_response(body)
    assert connected.get_records("orders") == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_get_records_explicit_records_path(connected: APIReader, session: MagicMock) -> None:
    body = {"data": {"items": [{"id": 1}]}, "other": [{"x": 1}, {"x": 2}]}
    session.request.return_value = make_response(body)
    assert connected.get_records("orders", records_path="data.items") == [{"id": 1}]


def test_get_records_single_object_becomes_one_record(
    connected: APIReader, session: MagicMock
) -> None:
    session.request.return_value = make_response({"id": 1, "name": "a"})
    assert connected.get_records("orders") == [{"id": 1, "name": "a"}]


# --- pagination ---


def test_get_records_follows_body_next_key(connected: APIReader, session: MagicMock) -> None:
    session.request.side_effect = [
        make_response({"items": [{"id": 1}], "next": f"{BASE}/orders?page=2"}),
        make_response({"items": [{"id": 2}], "next": None}, url=f"{BASE}/orders?page=2"),
    ]
    assert connected.get_records("orders", max_pages=5) == [{"id": 1}, {"id": 2}]
    assert session.request.call_count == 2


def test_get_records_follows_links_next_href(connected: APIReader, session: MagicMock) -> None:
    session.request.side_effect = [
        make_response({"items": [{"id": 1}], "links": {"next": {"href": "/v1/orders?page=2"}}}),
        make_response({"items": [{"id": 2}]}, url=f"{BASE}/orders?page=2"),
    ]
    assert connected.get_records("orders", max_pages=2) == [{"id": 1}, {"id": 2}]
    assert session.request.call_args.args[1] == "https://api.example.com/v1/orders?page=2"


def test_get_records_follows_link_header(connected: APIReader, session: MagicMock) -> None:
    session.request.side_effect = [
        make_response([{"id": 1}], links={"next": {"url": f"{BASE}/orders?page=2"}}),
        make_response([{"id": 2}], url=f"{BASE}/orders?page=2"),
    ]
    assert connected.get_records("orders", max_pages=2) == [{"id": 1}, {"id": 2}]


def test_get_records_stops_at_max_pages(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response(
        {"items": [{"id": 1}], "next": f"{BASE}/orders?page=2"}
    )
    records = connected.get_records("orders", max_pages=3)
    assert len(records) == 3
    assert session.request.call_count == 3


def test_get_records_default_reads_only_first_page(
    connected: APIReader, session: MagicMock
) -> None:
    session.request.return_value = make_response(
        {"items": [{"id": 1}], "next": f"{BASE}/orders?page=2"}
    )
    assert connected.get_records("orders") == [{"id": 1}]
    assert session.request.call_count == 1


def test_get_records_stops_when_no_next_link(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response({"items": [{"id": 1}]})
    assert connected.get_records("orders", max_pages=10) == [{"id": 1}]
    assert session.request.call_count == 1


def test_pagination_does_not_leave_base_host(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response(
        {"items": [{"id": 1}], "next": "https://evil.example.org/page2"}
    )
    with pytest.raises(ValueError, match="different host"):
        connected.get_records("orders", max_pages=2)


def test_get_records_uses_first_response_without_refetching(
    connected: APIReader, session: MagicMock
) -> None:
    first = make_response({"items": [{"id": 1}], "next": f"{BASE}/orders?page=2"})
    session.request.return_value = make_response({"items": [{"id": 2}]}, url=f"{BASE}/orders?page=2")
    records = connected.get_records("orders", max_pages=2, first_response=first)
    assert records == [{"id": 1}, {"id": 2}]
    assert session.request.call_count == 1  # only page two


# --- get_dataframe ---


def test_get_dataframe_returns_polars_frame(connected: APIReader, session: MagicMock) -> None:
    session.request.return_value = make_response(
        {"data": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}
    )
    expected = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    assert_frame_equal(connected.get_dataframe("orders"), expected)


# --- helpers ---


def test_find_record_arrays_reports_paths() -> None:
    body = {"a": [{"x": 1}], "b": {"c": [{"y": 1}, {"y": 2}]}, "d": [1, 2]}
    assert dict(find_record_arrays(body)) == {"a": [{"x": 1}], "b.c": [{"y": 1}, {"y": 2}]}


def test_extract_records_root_and_nested() -> None:
    assert extract_records([{"a": 1}], "$") == [{"a": 1}]
    assert extract_records({"a": {"b": [{"x": 1}]}}, "a.b") == [{"x": 1}]
