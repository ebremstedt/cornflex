from dataclasses import dataclass
from io import StringIO
from typing import Any, List, Optional
import chardet
import fnmatch
import paramiko
import polars as pl


@dataclass
class SFTPReader:
    hostname: str
    username: str
    pem_file: Optional[str] = None
    password: Optional[str] = None
    port: int = 22

    def __post_init__(self) -> None:
        self._client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        if not self.password and not self.pem_file:
            raise ValueError("Either password or pem_file must be provided")

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
                    source=StringIO(content),
                    has_header=False,
                    new_columns=column_names,
                )
            return pl.read_csv(source=StringIO(content))
        except Exception as e:
            print(f"Error getting {file_name}: {e}")
            return None

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
