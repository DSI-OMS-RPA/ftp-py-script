
# FTP Client Script

This project is an FTP/FTPS client that allows you to upload, download, and list files on an FTP server. It supports secure connections via FTPS (FTP over TLS).

## Features

- **Connect to FTP/FTPS servers** using a hostname, username, password, and optional TLS encryption.
- **Upload files** from your local system to the remote FTP server.
- **Download files** from the FTP server to your local system.
- **List files** in a specified remote directory.
- **Secure**: Optionally use TLS to secure your FTP connection.

## Requirements

- Python 3.6+
- Required libraries:
    - `ftplib`
    - `ssl` (for FTPS)

## Setup

1. Clone the repository or download the script.
2. Install any necessary dependencies (if applicable).
3. Edit the `main()` function in the script to provide your FTP server details.

## Usage

Here's an example of how you can use the FTP client:

```python
from ftp_client import FTPClient

def main():
    ftp_client = FTPClient(hostname="ftp.example.com", username="user", password="pass", use_tls=True)

    # Connect to the server
    ftp_client.connect()

    # Upload a file
    ftp_client.upload_file("local/path/to/file.txt", "/remote/path/file.txt")

    # Download a file
    ftp_client.download_file("/remote/path/file.txt", "local/path/to/file.txt")

    # List files in the remote directory
    ftp_client.list_files("/remote/path/")

    # Disconnect from the server
    ftp_client.disconnect()

if __name__ == "__main__":
    main()
```

### Commands

- **Connect**: Establish a connection with the FTP/FTPS server.
- **Upload File**: Upload a file from the local machine to the FTP server.
- **Download File**: Download a file from the FTP server to the local machine.
- **List Files**: List all files in a specified remote directory.
- **Disconnect**: Close the connection to the FTP server.

## License

This project is released under the MIT License.
