from ftp_client import FTPClient

def main():

    ftp_client = FTPClient(hostname="ftp.example.com", username="user", password="pass", use_tls=True)

    # Connect to the server
    ftp_client.connect()

    # Upload a file
    ftp_client.upload_file("local/path/to/file.txt", "/remote/path/file.txt")

    # Download a file
    ftp_client.download_file("/remote/path/file.txt", "local/path/to/file.txt")

    # List files
    ftp_client.list_files("/remote/path/")

    # Disconnect from the server
    ftp_client.disconnect()

if __name__ == "__main__":
    main()
