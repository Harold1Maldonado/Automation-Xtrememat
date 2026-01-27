import os
import paramiko


def sftp_upload(local_path: str, remote_dir: str):
    host = os.environ["FTP_HOST"]
    port = int(os.environ.get("FTP_PORT", "22"))
    user = os.environ["FTP_USER"]
    password = os.environ["FTP_PASS"]

    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    filename = os.path.basename(local_path)
    remote_path = f"{remote_dir.rstrip('/')}/{filename}"

    sftp.put(local_path, remote_path)

    sftp.close()
    transport.close()
