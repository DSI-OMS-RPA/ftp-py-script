from ftp_client import FTPClient
import logging

def main():

    # Configure logging for the main script
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Initialize FTP client with credentials and connection details
    ftp_client = FTPClient(hostname="ftp.example.com", username="user", password="pass", use_tls=True)

    try:
        # Connect to the server
        ftp_client.connect()

        # Upload a file
        if ftp_client.upload_file("local/path/to/file.txt", "/remote/path/file.txt"):
            logger.info("File uploaded successfully.")
        else:
            logger.error("File upload failed.")

        # Download a file
        if ftp_client.download_file("/remote/path/file.txt", "local/path/to/file.txt"):
            logger.info("File downloaded successfully.")
        else:
            logger.error("File download failed.")

        # List files in the remote directory
        files = ftp_client.list_files("/remote/path/")
        logger.info(f"Files in remote directory: {files}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Ensure the client disconnects even if an error occurs
        ftp_client.disconnect()

if __name__ == "__main__":
    main()
