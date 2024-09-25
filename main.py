import os
from dotenv import load_dotenv
from ftp_client import FTPClient
import logging

# Load environment variables from the .env file
load_dotenv()

def main():
    # Configure logging for the main script
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Retrieve FTP connection details from environment variables
    hostname = os.getenv("FTP_HOSTNAME")
    username = os.getenv("FTP_USERNAME")
    password = os.getenv("FTP_PASSWORD")
    use_tls = os.getenv("FTP_USE_TLS", "True").lower() == "true"  # Default to True if not provided

    # Initialize FTP client with credentials and connection details
    ftp_client = FTPClient(hostname=hostname, username=username, password=password, use_tls=use_tls)

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
