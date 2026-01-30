import os
import socket
import time
from typing import Optional

import paramiko


def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir: str) -> None:

    remote_dir = remote_dir.rstrip("/")
    if not remote_dir:
        return

    parts = remote_dir.strip("/").split("/")
    path = ""
    for part in parts:
        path += "/" + part
        try:
            sftp.stat(path)
        except FileNotFoundError:
            sftp.mkdir(path)


def sftp_upload(
    local_path: str,
    remote_dir: str,
    retries: int = 4,
    delay_sec: int = 5,
    timeout_sec: int = 15,
    ensure_dir: bool = False,
    atomic: bool = True,
) -> None:

    host = os.environ["FTP_HOST"]
    port = int(os.environ.get("FTP_PORT", "22"))
    user = os.environ["FTP_USER"]
    password = os.environ["FTP_PASS"]

    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        transport: Optional[paramiko.Transport] = None
        sftp: Optional[paramiko.SFTPClient] = None
        sock: Optional[socket.socket] = None

        try:
            ip = socket.gethostbyname(host)

            sock = socket.create_connection((ip, port), timeout=timeout_sec)

            transport = paramiko.Transport(sock)
            transport.banner_timeout = timeout_sec
            transport.auth_timeout = timeout_sec
            transport.set_keepalive(30)

            transport.connect(username=user, password=password)

            sftp = paramiko.SFTPClient.from_transport(transport)

            if ensure_dir:
                _ensure_remote_dir(sftp, remote_dir)

            filename = os.path.basename(local_path)
            remote_path = f"{remote_dir.rstrip('/')}/{filename}"

            if atomic:
                tmp_remote = remote_path + ".tmp"
                sftp.put(local_path, tmp_remote)
                sftp.rename(tmp_remote, remote_path)
            else:
                sftp.put(local_path, remote_path)

            return

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay_sec)
            else:
                raise

        finally:
            try:
                if sftp is not None:
                    sftp.close()
            except Exception:
                pass
            try:
                if transport is not None:
                    transport.close()
            except Exception:
                pass
            try:
                if sock is not None:
                    sock.close()
            except Exception:
                pass

    if last_err:
        raise last_err
