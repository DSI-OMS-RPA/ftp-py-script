import os
from dotenv import load_dotenv
from ftp_client import FTPClient
import logging

# Load environment variables from the .env file
load_dotenv()

def main():
    """
    Main function to manage FTP operations such as connecting to the server,
    uploading and downloading files, and listing directory contents.
    Environment variables are loaded for configuration, and logging is used
    for tracking success or failure of operations.
    """
    # Configure logging for the main script
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Retrieve FTP connection details from environment variables
    hostname = os.getenv("FTP_HOSTNAME")
    username = os.getenv("FTP_USERNAME")
    password = os.getenv("FTP_PASSWORD")
    use_tls = os.getenv("FTP_USE_TLS", "True").lower() == "true"  # Default to True if not provided

    # Validate that the necessary environment variables are set
    if not hostname or not username or not password:
        logger.error("FTP connection details are not properly set in the environment variables.")
        return

    # Initialize FTP client with credentials and connection details
    ftp_client = FTPClient(hostname=hostname, username=username, password=password, use_tls=use_tls)

    try:
        # Connect to the server
        ftp_client.connect()

        # Upload a file
        logger.info("Uploading file...")
        ftp_client.upload_file("local/path/to/file.txt", "/remote/path/file.txt")
        logger.info("File uploaded successfully.")

        # Download a file
        logger.info("Downloading file...")
        ftp_client.download_file("/remote/path/file.txt", "local/path/to/file.txt")
        logger.info("File downloaded successfully.")

        # List files in the remote directory
        logger.info("Listing files in remote directory...")
        files = ftp_client.list_files("/remote/path/")
        logger.info(f"Files in remote directory: {files}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        # Ensure the client disconnects even if an error occurs
        ftp_client.disconnect()
        logger.info("Disconnected from the server.")

if __name__ == "__main__":
    main()
