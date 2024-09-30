from contextlib import contextmanager
import hashlib
import os
import ftplib
from retrying import retry  # To handle retrying failed operations
import threading  # For thread-safe connection pool
import logging  # For logging errors and information
from tqdm import tqdm  # For tracking progress
from concurrent.futures import ThreadPoolExecutor  # For parallel file transfers

# Custom exceptions for FTP errors
class FTPConnectionError(Exception):
    """Custom exception for FTP connection errors."""
    pass

class FTPTransferError(Exception):
    """Custom exception for FTP transfer errors."""
    pass

class FTPClient:
    """
    A robust FTP/FTPS client class that supports connection pooling, timeout handling,
    retry mechanisms, transfer progress tracking, and parallel file transfers.
    """

    def __init__(self, hostname, username, password, use_tls=False, max_connections=5, timeout=10, log_level=logging.INFO):
        """
        Initializes the FTPClient with server credentials and connection settings.

        :param hostname: The FTP server hostname.
        :param username: FTP account username.
        :param password: FTP account password.
        :param use_tls: Whether to use FTPS (TLS) or plain FTP. Default is False (use FTPS).
        :param max_connections: Maximum number of FTP connections to pool.
        :param timeout: Timeout for the FTP connections in seconds.
        :param log_level: Level of logging (default is INFO).
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout = timeout
        self.connection_pool = []  # Pool of reusable FTP connections
        self.max_connections = max_connections
        self.lock = threading.Lock()  # Ensures thread-safe access to the connection pool

        # Set up logging to track client operations
        self.logger.setLevel(log_level)

    def _create_connection(self):
        """
        Creates a new FTP or FTPS connection.

        :return: A new FTP or FTPS connection.
        :raises FTPConnectionError: If connection to the server fails.
        """
        try:
            if self.use_tls:
                ftp = ftplib.FTP_TLS(self.hostname, timeout=self.timeout)
                ftp.login(self.username, self.password)
                ftp.prot_p()  # Switch to secure data connection
            else:
                ftp = ftplib.FTP(self.hostname, timeout=self.timeout)
                ftp.login(self.username, self.password)
            return ftp
        except Exception as e:
            raise FTPConnectionError(f"Error connecting to FTP server: {e}")

    def _get_connection(self):
        """
        Retrieves an available connection from the pool or creates a new one if the pool is empty.

        :return: An FTP connection.
        """
        with self.lock:
            if len(self.connection_pool) > 0:
                return self.connection_pool.pop()
            else:
                return self._create_connection()

    def _release_connection(self, conn):
        """
        Releases a connection back to the pool or closes it if the pool is full.

        :param conn: The FTP connection to release.
        """
        with self.lock:
            if len(self.connection_pool) < self.max_connections:
                self.connection_pool.append(conn)
            else:
                conn.quit()  # Close the connection if the pool is full

    @contextmanager
    def ftp_connection(self):
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._release_connection(conn)

    def connect(self):
        """
        Pre-warm the connection pool by establishing a set number of FTP connections.
        """
        for _ in range(self.max_connections):
            self.connection_pool.append(self._create_connection())

    def disconnect(self):
        """
        Close all connections in the pool when done.
        """
        while self.connection_pool:
            conn = self.connection_pool.pop()
            conn.quit()  # Close each connection in the pool
        self.logger.info("Disconnected from FTP server.")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def upload_file(self, local_file_path, remote_file_path, progress_callback=None):
        """
        Uploads a file to the FTP server with retry and progress tracking.

        :param local_file_path: Path to the local file to be uploaded.
        :param remote_file_path: Path on the remote server where the file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :raises FTPTransferError: If the file upload fails after retries.
        """
        with self.ftp_connection() as ftp:
            try:
                with open(local_file_path, 'rb') as file:
                    total_size = os.path.getsize(local_file_path)
                    # Display a progress bar while uploading
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                        def progress_callback(block):
                            pbar.update(len(block))
                        ftp.storbinary(f"STOR {remote_file_path}", file, callback=progress_callback)

                self.logger.info(f"Uploaded: {local_file_path} to {remote_file_path}")
                self._release_connection(ftp)
            except Exception as e:
                raise FTPTransferError(f"Failed to upload file {local_file_path}: {e}")
            finally:
                self._release_connection(ftp)

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def download_file(self, remote_file_path, local_file_path, progress_callback=None):
        """
        Downloads a file from the FTP server with retry and progress tracking.

        :param remote_file_path: Path to the file on the FTP server.
        :param local_file_path: Local path where the downloaded file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :raises FTPTransferError: If the file download fails after retries.
        """
        try:
            ftp = self._get_connection()

            with open(local_file_path, 'wb') as file:
                total_size = ftp.size(remote_file_path)
                # Display a progress bar while downloading
                with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                    def progress_callback(block):
                        file.write(block)
                        pbar.update(len(block))
                    ftp.retrbinary(f"RETR {remote_file_path}", progress_callback)

            self.logger.info(f"Downloaded: {remote_file_path} to {local_file_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to download file {remote_file_path}: {e}")

    def list_files(self, remote_path, only_files=True):
        """
        Lists the files in a specified directory on the FTP server.

        :param remote_path: The directory path on the remote server.
        :param only_files: Whether to list only files (True) or include both files and directories (False).
        :return: A list of filenames in the directory.
        :raises FTPTransferError: If listing files fails.
        """
        try:
            ftp = self._get_connection()
            files = ftp.nlst(remote_path)  # List files and directories in the directory
            if only_files:
                files = [f for f in files if not self._is_directory(ftp, f)]  # Filter out directories

            self._release_connection(ftp)
            return files
        except Exception as e:
            raise FTPTransferError(f"Failed to list files in {remote_path}: {e}")

    def _is_directory(self, ftp, item):
        """
        Checks if the given item is a directory.

        :param ftp: The FTP connection.
        :param item: The item path to check.
        :return: True if the item is a directory, False otherwise.
        """
        current = ftp.pwd()  # Save the current working directory
        try:
            ftp.cwd(item)  # Try to change to the item as if it were a directory
            ftp.cwd(current)  # Change back to the original directory
            return True
        except ftplib.error_perm:  # If permission is denied, it's a file
            return False

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def rename_file(self, old_remote_path, new_remote_path):
        """
        Renames a file on the FTP server.

        :param old_remote_path: The current path of the remote file.
        :param new_remote_path: The new path for the remote file.
        :raises FTPTransferError: If the file renaming fails after retries.
        """
        try:
            ftp = self._get_connection()
            ftp.rename(old_remote_path, new_remote_path)
            self.logger.info(f"Renamed file from {old_remote_path} to {new_remote_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to rename file from {old_remote_path} to {new_remote_path}: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def delete_file(self, remote_file_path):
        """
        Deletes a file from the FTP server.

        :param remote_file_path: The path of the remote file to be deleted.
        :raises FTPTransferError: If the file deletion fails after retries.
        """
        try:
            ftp = self._get_connection()
            ftp.delete(remote_file_path)
            self.logger.info(f"Deleted file: {remote_file_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to delete file {remote_file_path}: {e}")

    def check_file_exists(self, remote_file_path):
        """
        Checks if a file exists on the FTP server.

        :param remote_file_path: The path of the remote file to check.
        :return: True if the file exists, False otherwise.
        """
        try:
            ftp = self._get_connection()
            files = ftp.nlst(os.path.dirname(remote_file_path))  # List files in the directory
            exists = os.path.basename(remote_file_path) in files
            self._release_connection(ftp)
            return exists
        except Exception as e:
            self.logger.error(f"Failed to check if file exists {remote_file_path}: {e}")
            return False

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def create_directory(self, remote_directory_path):
        """
        Creates a new directory on the FTP server.

        :param remote_directory_path: The path of the remote directory to be created.
        :raises FTPTransferError: If the directory creation fails after retries.
        """
        try:
            ftp = self._get_connection()
            ftp.mkd(remote_directory_path)
            self.logger.info(f"Created directory: {remote_directory_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to create directory {remote_directory_path}: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def remove_directory(self, remote_directory_path):
        """
        Removes a directory from the FTP server.

        :param remote_directory_path: The path of the remote directory to be removed.
        :raises FTPTransferError: If the directory removal fails after retries.
        """
        try:
            ftp = self._get_connection()
            ftp.rmd(remote_directory_path)
            self.logger.info(f"Removed directory: {remote_directory_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to remove directory {remote_directory_path}: {e}")

    def change_directory(self, remote_directory_path):
        """
        Changes the current working directory on the FTP server.

        :param remote_directory_path: The path of the remote directory to change to.
        :raises FTPTransferError: If changing directory fails after retries.
        """
        try:
            ftp = self._get_connection()
            ftp.cwd(remote_directory_path)
            self.logger.info(f"Changed directory to: {remote_directory_path}")
            self._release_connection(ftp)
        except Exception as e:
            raise FTPTransferError(f"Failed to change directory to {remote_directory_path}: {e}")

    def calculate_md5(self, file_path):
        """
        Calculates the MD5 checksum of a file.

        :param file_path: Path to the file for which to calculate the checksum.
        :return: The MD5 checksum as a hexadecimal string.
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def verify_file_integrity(self, local_file_path, remote_file_path):
        """
        Verifies the integrity of a file by comparing local and remote checksums.

        :param local_file_path: The path of the local file.
        :param remote_file_path: The path of the remote file on the FTP server.
        :raises FTPTransferError: If verification fails.
        """
        local_checksum = self.calculate_md5(local_file_path)
        try:
            ftp = self._get_connection()
            # Assuming we can retrieve the remote file's checksum somehow
            remote_checksum = ftp.sendcmd(f'SITE MD5 {remote_file_path}')
            self._release_connection(ftp)

            if local_checksum != remote_checksum.split(' ')[1]:  # Parse the response appropriately
                raise FTPTransferError(f"Checksum mismatch for {local_file_path} and {remote_file_path}")
            self.logger.info(f"Checksum verified for {local_file_path} and {remote_file_path}")

        except Exception as e:
            raise FTPTransferError(f"Failed to verify integrity for {local_file_path}: {e}")

    def parallel_upload(self, files):
        """
        Uploads multiple files in parallel using multiple threads.

        :param files: A list of tuples with local and remote file paths [(local, remote), ...].
        """
        with ThreadPoolExecutor(max_workers=self.max_connections) as executor:
            futures = [executor.submit(self.upload_file, local, remote) for local, remote in files]
            for future in futures:
                try:
                    future.result()  # Wait for each upload to complete
                except FTPTransferError as e:
                    self.logger.error(f"Error during parallel upload: {e}")

    def parallel_download(self, files):
        """
        Downloads multiple files in parallel using multiple threads.

        :param files: A list of tuples with remote and local file paths [(remote, local), ...].
        """
        with ThreadPoolExecutor(max_workers=self.max_connections) as executor:
            futures = [executor.submit(self.download_file, remote, local) for remote, local in files]
            for future in futures:
                try:
                    future.result()  # Wait for each download to complete
                except FTPTransferError as e:
                    self.logger.error(f"Error during parallel download: {e}")
