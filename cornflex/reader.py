import fnmatch
import io
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from typing import Any, Generator, List, Optional

import chardet
import paramiko
import polars as pl


@dataclass
class SFTPReader:
    hostname: str
    username: str
    pem_file: Optional[str] = None
    password: Optional[str] = None
    port: int = 22
    encoding: str = "utf-8"
    preferred_keys: list[str] | None = None
    preferred_kex: list[str] | None = None

    def __post_init__(self) -> None:
        self._client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        if not self.password and not self.pem_file:
            raise ValueError("Either password or pem_file must be provided")

        if self.preferred_keys:
            paramiko.Transport._preferred_keys = (
                tuple(self.preferred_keys) + paramiko.Transport._preferred_keys
            )
        if self.preferred_kex:
            paramiko.Transport._preferred_kex = (
                tuple(self.preferred_kex) + paramiko.Transport._preferred_kex
            )

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict[str, Any] = {
            "hostname": self.hostname,
            "port": self.port,
            "username": self.username,
            "timeout": 30,
        }

        if self.pem_file:
            private_key = paramiko.RSAKey.from_private_key_file(self.pem_file)
            connect_kwargs["pkey"] = private_key
        else:
            connect_kwargs["password"] = self.password

        self._client.connect(**connect_kwargs)
        self._sftp = self._client.open_sftp()

    def close(self) -> None:
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()

    def listdir_attr(self, remote_path: str = ".") -> list[paramiko.SFTPAttributes]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._sftp.listdir_attr(remote_path)

    def get_files(
        self,
        remote_path: str = ".",
        file_pattern: str = "*",
    ) -> List[str]:
        self.connect()
        try:
            all_files: List[str] = self._sftp.listdir(remote_path)
            return [f for f in all_files if fnmatch.fnmatch(name=f, pat=file_pattern)]
        finally:
            self.close()

    def get_csv_file(
        self,
        file_name: str,
        remote_path: str = ".",
        column_names: Optional[list[str]] = None,
    ) -> Optional[pl.DataFrame]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            remote_file_path = f"{remote_path.rstrip('/')}/{file_name}"
            with self._sftp.file(remote_file_path, "r") as file:
                content = file.read().decode("utf-8")

            if column_names:
                return pl.read_csv(
                    source=io.StringIO(content),
                    has_header=False,
                    new_columns=column_names,
                )
            return pl.read_csv(source=io.StringIO(content))
        except Exception as e:
            print(f"Error getting {file_name}: {e}")
            return None

    def get_zip_csv_batched(
        self,
        sftp_path: str,
        separator: str = "\t",
        column_names: list[str] | None = None,
        batch_size: int = 50_000,
    ) -> Generator[pl.DataFrame, None, None]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        zip_fd, zip_path = tempfile.mkstemp(suffix=".zip")
        os.close(zip_fd)
        csv_fd, csv_path = tempfile.mkstemp(suffix=".csv")
        os.close(csv_fd)

        try:
            with self._sftp.open(sftp_path, "rb") as remote_file, open(zip_path, "wb") as zip_file:
                shutil.copyfileobj(remote_file, zip_file, length=1024 * 1024)

            with zipfile.ZipFile(zip_path) as zf:
                name = zf.namelist()[0]
                with zf.open(name) as zf_entry, open(csv_path, "w", encoding="utf-8") as csv_file:
                    for line in io.TextIOWrapper(zf_entry, encoding=self.encoding):
                        csv_file.write(line)

            os.unlink(zip_path)
            zip_path = ""

            reader = pl.read_csv_batched(
                csv_path,
                separator=separator,
                quote_char='"',
                infer_schema_length=0,
                truncate_ragged_lines=True,
                batch_size=batch_size,
                has_header=column_names is None,
                new_columns=column_names,
            )
            while True:
                batches = reader.next_batches(1)
                if not batches:
                    break
                yield batches[0]

        finally:
            for path in filter(None, (zip_path, csv_path)):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

    def get_xml_file_to_string(
        self,
        file_name: str,
        remote_path: str = ".",
    ) -> Optional[str]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            remote_file_path = f"{remote_path.rstrip('/')}/{file_name}"
            with self._sftp.file(remote_file_path, "r") as file:
                content = file.read().decode("utf-8")
            return content
        except Exception as e:
            print(f"Error getting {file_name}: {e}")
            return None

    def file_to_string(
        self,
        file_name: str,
        remote_path: str = ".",
        encoding: Optional[str] = None,
    ) -> Optional[str]:
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first.")

        try:
            remote_file_path = f"{remote_path.rstrip('/')}/{file_name}"
            with self._sftp.file(remote_file_path, "rb") as file:
                raw_bytes = file.read()

            if encoding is None:
                detected = chardet.detect(raw_bytes)
                encoding = detected["encoding"]

            content = raw_bytes.decode(encoding)
            return content

        except Exception as e:
            print(f"Error getting {file_name}: {e}")
            return None
