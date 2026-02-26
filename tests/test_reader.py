from io import StringIO
from unittest.mock import MagicMock, patch, PropertyMock
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from cornflex.reader import SFTPReader


@pytest.fixture
def reader_password() -> SFTPReader:
    return SFTPReader(hostname="host", username="user", password="pass")


@pytest.fixture
def reader_pem() -> SFTPReader:
    return SFTPReader(hostname="host", username="user", pem_file="/path/to/key.pem")


@pytest.fixture
def connected_reader(reader_password: SFTPReader) -> SFTPReader:
    reader_password._sftp = MagicMock()
    return reader_password


# --- connect ---


def test_connect_raises_without_auth() -> None:
    reader = SFTPReader(hostname="host", username="user")
    with pytest.raises(
        ValueError, match="Either password or pem_file must be provided"
    ):
        reader.connect()


@patch("cornflex.reader.paramiko.SSHClient")
def test_connect_with_password(
    mock_ssh: MagicMock, reader_password: SFTPReader
) -> None:
    reader_password.connect()
    mock_ssh().connect.assert_called_once()
    call_kwargs = mock_ssh().connect.call_args.kwargs
    assert call_kwargs["password"] == "pass"
    assert "pkey" not in call_kwargs


@patch("cornflex.reader.paramiko.RSAKey.from_private_key_file")
@patch("cornflex.reader.paramiko.SSHClient")
def test_connect_with_pem(
    mock_ssh: MagicMock, mock_rsa: MagicMock, reader_pem: SFTPReader
) -> None:
    reader_pem.connect()
    mock_rsa.assert_called_once_with("/path/to/key.pem")
    call_kwargs = mock_ssh().connect.call_args.kwargs
    assert "pkey" in call_kwargs
    assert "password" not in call_kwargs


# --- close ---


def test_close_calls_sftp_and_client(reader_password: SFTPReader) -> None:
    reader_password._sftp = MagicMock()
    reader_password._client = MagicMock()
    reader_password.close()
    reader_password._sftp.close.assert_called_once()
    reader_password._client.close.assert_called_once()


def test_close_without_connection_does_not_raise(reader_password: SFTPReader) -> None:
    reader_password.close()


# --- get_files ---


@patch("cornflex.reader.paramiko.SSHClient")
def test_get_files_returns_filtered(
    mock_ssh: MagicMock, reader_password: SFTPReader
) -> None:
    mock_sftp = MagicMock()
    mock_sftp.listdir.return_value = ["orders.csv", "users.csv", "config.json"]
    mock_ssh().open_sftp.return_value = mock_sftp

    result = reader_password.get_files(remote_path="/data", file_pattern="*.csv")
    assert result == ["orders.csv", "users.csv"]


@patch("cornflex.reader.paramiko.SSHClient")
def test_get_files_no_match(mock_ssh: MagicMock, reader_password: SFTPReader) -> None:
    mock_sftp = MagicMock()
    mock_sftp.listdir.return_value = ["config.json", "notes.txt"]
    mock_ssh().open_sftp.return_value = mock_sftp

    result = reader_password.get_files(file_pattern="*.csv")
    assert result == []


# --- get_csv_file ---


def test_get_csv_file_raises_when_not_connected(reader_password: SFTPReader) -> None:
    with pytest.raises(RuntimeError, match="Not connected"):
        reader_password.get_csv_file("file.csv")


def test_get_csv_file_returns_dataframe(connected_reader: SFTPReader) -> None:
    csv_content = b"id,name\n1,Alice\n2,Bob"
    mock_file = MagicMock()
    mock_file.read.return_value = csv_content
    mock_file.__enter__ = lambda s: s
    mock_file.__exit__ = MagicMock(return_value=False)
    connected_reader._sftp.file.return_value = mock_file

    result = connected_reader.get_csv_file("data.csv", remote_path="/data")
    expected = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    assert_frame_equal(result, expected)


def test_get_csv_file_with_column_names(connected_reader: SFTPReader) -> None:
    csv_content = b"1,Alice\n2,Bob"
    mock_file = MagicMock()
    mock_file.read.return_value = csv_content
    mock_file.__enter__ = lambda s: s
    mock_file.__exit__ = MagicMock(return_value=False)
    connected_reader._sftp.file.return_value = mock_file

    result = connected_reader.get_csv_file("data.csv", column_names=["id", "name"])
    assert result.columns == ["id", "name"]


def test_get_csv_file_returns_none_on_error(connected_reader: SFTPReader) -> None:
    connected_reader._sftp.file.side_effect = Exception("boom")
    result = connected_reader.get_csv_file("bad.csv")
    assert result is None


# --- get_xml_file_to_string ---


def test_get_xml_raises_when_not_connected(reader_password: SFTPReader) -> None:
    with pytest.raises(RuntimeError, match="Not connected"):
        reader_password.get_xml_file_to_string("file.xml")


def test_get_xml_returns_string(connected_reader: SFTPReader) -> None:
    xml_content = b"<root><item>1</item></root>"
    mock_file = MagicMock()
    mock_file.read.return_value = xml_content
    mock_file.__enter__ = lambda s: s
    mock_file.__exit__ = MagicMock(return_value=False)
    connected_reader._sftp.file.return_value = mock_file

    result = connected_reader.get_xml_file_to_string("feed.xml", remote_path="/exports")
    assert result == "<root><item>1</item></root>"


def test_get_xml_returns_none_on_error(connected_reader: SFTPReader) -> None:
    connected_reader._sftp.file.side_effect = Exception("boom")
    result = connected_reader.get_xml_file_to_string("bad.xml")
    assert result is None


# --- file_to_string ---


def test_file_to_string_raises_when_not_connected(reader_password: SFTPReader) -> None:
    with pytest.raises(RuntimeError, match="Not connected"):
        reader_password.file_to_string("file.txt")


def test_file_to_string_with_explicit_encoding(connected_reader: SFTPReader) -> None:
    mock_file = MagicMock()
    mock_file.read.return_value = "héllo".encode("latin-1")
    mock_file.__enter__ = lambda s: s
    mock_file.__exit__ = MagicMock(return_value=False)
    connected_reader._sftp.file.return_value = mock_file

    result = connected_reader.file_to_string("report.txt", encoding="latin-1")
    assert result == "héllo"


def test_file_to_string_auto_detects_encoding(connected_reader: SFTPReader) -> None:
    mock_file = MagicMock()
    mock_file.read.return_value = "hello world".encode("utf-8")
    mock_file.__enter__ = lambda s: s
    mock_file.__exit__ = MagicMock(return_value=False)
    connected_reader._sftp.file.return_value = mock_file

    result = connected_reader.file_to_string("report.txt")
    assert result == "hello world"


def test_file_to_string_returns_none_on_error(connected_reader: SFTPReader) -> None:
    connected_reader._sftp.file.side_effect = Exception("boom")
    result = connected_reader.file_to_string("bad.txt")
    assert result is None
