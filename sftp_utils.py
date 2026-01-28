import os
import socket
import time
import paramiko


def sftp_upload(local_path: str, remote_dir: str, retries: int = 3, delay_sec: int = 5):
    host = os.environ["FTP_HOST"]
    port = int(os.environ.get("FTP_PORT", "22"))
    user = os.environ["FTP_USER"]
    password = os.environ["FTP_PASS"]

    last_err = None

    for attempt in range(1, retries + 1):
        try:
            # Forzar resolución IPv4
            ip = socket.gethostbyname(host)

            transport = paramiko.Transport((ip, port))
            try:
                transport.connect(username=user, password=password)
                sftp = paramiko.SFTPClient.from_transport(transport)
                try:
                    filename = os.path.basename(local_path)
                    remote_path = f"{remote_dir.rstrip('/')}/{filename}"
                    sftp.put(local_path, remote_path)
                    return
                finally:
                    sftp.close()
            finally:
                transport.close()

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay_sec)
            else:
                raise last_err
