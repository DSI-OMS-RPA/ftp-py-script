import os
import ftplib
import logging
from typing import List, Optional

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FTPClient:
    def __init__(self, hostname: str, username: str, password: str, port: int = 21, use_tls: bool = False, passive_mode: bool = True):
        """
        Initializes the FTP/FTPS client.

        Args:
            hostname (str): The hostname or IP address of the FTP server.
            username (str): FTP username.
            password (str): FTP password.
            port (int, optional): The port to connect to. Defaults to 21.
            use_tls (bool, optional): Whether to use FTPS (FTP over TLS). Defaults to False.
            passive_mode (bool, optional): Whether to use passive mode. Defaults to True.
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.use_tls = use_tls
        self.passive_mode = passive_mode
        self.ftp = None

    def connect(self):
        """Connects to the FTP/FTPS server."""
        try:
            if self.use_tls:
                self.ftp = ftplib.FTP_TLS()
                logger.info("Using FTPS (TLS) connection.")
            else:
                self.ftp = ftplib.FTP()
                logger.info("Using FTP connection.")

            self.ftp.connect(self.hostname, self.port)
            self.ftp.login(self.username, self.password)

            if self.use_tls:
                self.ftp.prot_p()  # Secure the data connection for FTPS

            self.ftp.set_pasv(self.passive_mode)
            logger.info(f"Connected to FTP server at {self.hostname}:{self.port}")
        except ftplib.all_errors as e:
            logger.error(f"Error connecting to FTP server: {e}")
            raise

    def disconnect(self):
        """Disconnects from the FTP server."""
        if self.ftp:
            try:
                self.ftp.quit()
                logger.info("Disconnected from the FTP server.")
            except ftplib.all_errors as e:
                logger.error(f"Error disconnecting from FTP server: {e}")

    def upload_file(self, local_file_path: str, remote_file_path: str) -> bool:
        """
        Uploads a file to the FTP server.

        Args:
            local_file_path (str): The path to the local file to upload.
            remote_file_path (str): The path on the FTP server to upload the file to.

        Returns:
            bool: True if the file is uploaded successfully, False otherwise.
        """
        try:
            with open(local_file_path, 'rb') as file:
                self.ftp.storbinary(f'STOR {remote_file_path}', file)
                logger.info(f"File uploaded to {remote_file_path}")
                return True
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_file_path}")
            return False
        except ftplib.all_errors as e:
            logger.error(f"FTP error during upload: {e}")
            return False

    def download_file(self, remote_file_path: str, local_file_path: str) -> bool:
        """
        Downloads a file from the FTP server.

        Args:
            remote_file_path (str): The path to the file on the FTP server.
            local_file_path (str): The path to save the downloaded file locally.

        Returns:
            bool: True if the file is downloaded successfully, False otherwise.
        """
        try:
            with open(local_file_path, 'wb') as file:
                self.ftp.retrbinary(f'RETR {remote_file_path}', file.write)
                logger.info(f"File downloaded from {remote_file_path} to {local_file_path}")
                return True
        except ftplib.all_errors as e:
            logger.error(f"FTP error during download: {e}")
            return False

    def delete_file(self, remote_file_path: str) -> bool:
        """
        Deletes a file from the FTP server.

        Args:
            remote_file_path (str): The path to the file on the FTP server.

        Returns:
            bool: True if the file is deleted successfully, False otherwise.
        """
        try:
            self.ftp.delete(remote_file_path)
            logger.info(f"File {remote_file_path} deleted from the server.")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP error during deletion: {e}")
            return False

    def rename_file(self, old_file_path: str, new_file_path: str) -> bool:
        """
        Renames a file on the FTP server.

        Args:
            old_file_path (str): The current path to the file.
            new_file_path (str): The new path for the file.

        Returns:
            bool: True if the file is renamed successfully, False otherwise.
        """
        try:
            self.ftp.rename(old_file_path, new_file_path)
            logger.info(f"File renamed from {old_file_path} to {new_file_path}")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP error during renaming: {e}")
            return False

    def list_files(self, remote_folder: str) -> List[str]:
        """
        Lists files in a directory on the FTP server.

        Args:
            remote_folder (str): The path to the remote directory.

        Returns:
            List[str]: A list of file names in the directory.
        """
        try:
            file_list = self.ftp.nlst(remote_folder)
            logger.info(f"Files in {remote_folder}: {file_list}")
            return file_list
        except ftplib.all_errors as e:
            logger.error(f"FTP error during listing files: {e}")
            return []

    def check_file_exists(self, remote_file_path: str) -> bool:
        """
        Checks if a file exists on the FTP server.

        Args:
            remote_file_path (str): The path to the file on the FTP server.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            self.ftp.size(remote_file_path)
            logger.info(f"File {remote_file_path} exists on the server.")
            return True
        except ftplib.error_perm:
            logger.info(f"File {remote_file_path} does not exist on the server.")
            return False
        except ftplib.all_errors as e:
            logger.error(f"FTP error checking file existence: {e}")
            return False

    def create_directory(self, remote_folder: str) -> bool:
        """
        Creates a directory on the FTP server.

        Args:
            remote_folder (str): The path to the directory to be created.

        Returns:
            bool: True if the directory is created successfully, False otherwise.
        """
        try:
            self.ftp.mkd(remote_folder)
            logger.info(f"Directory {remote_folder} created on the server.")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP error during directory creation: {e}")
            return False

    def remove_directory(self, remote_folder: str) -> bool:
        """
        Removes a directory from the FTP server.

        Args:
            remote_folder (str): The path to the directory to be removed.

        Returns:
            bool: True if the directory is removed successfully, False otherwise.
        """
        try:
            self.ftp.rmd(remote_folder)
            logger.info(f"Directory {remote_folder} removed from the server.")
            return True
        except ftplib.all_errors as e:
            logger.error(f"FTP error during directory removal: {e}")
            return False
