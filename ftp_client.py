import os
import ftplib
import hashlib
from retrying import retry  # To handle retrying failed operations
import threading  # For thread-safe connection pool
import logging  # For logging errors and information
from tqdm import tqdm  # For tracking progress
from contextlib import contextmanager
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

    def __init__(self, hostname, username, password, use_tls=False, max_connections=5, timeout=10, log_level=logging.INFO, retry_attempts=5, retry_multiplier=1000, retry_max=10000):
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

        self.retry_attempts = retry_attempts
        self.retry_multiplier = retry_multiplier
        self.retry_max = retry_max

        # Set up logging to track client operations
        logging.basicConfig(level=log_level)
        self.logger = logging.getLogger(__name__)

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
                conn = self.connection_pool.pop()
                # Check if the connection is still alive
                try:
                    conn.voidcmd("NOOP")  # Send a NOOP command to keep the connection alive
                except Exception:
                    self.logger.warning("Recreating a dropped FTP connection.")
                    conn = self._create_connection()  # Recreate if the connection is broken
                return conn
            else:
                return self._create_connection()

    def _release_connection(self, conn, auto_release=True):
        """
        Releases a connection back to the pool or closes it if the pool is full, based on auto_release.

        :param conn: The FTP connection to release.
        :param auto_release: Whether to release the connection back to the pool (default is True).
        """
        if auto_release:
            with self.lock:
                if len(self.connection_pool) < self.max_connections:
                    self.connection_pool.append(conn)
                else:
                    conn.quit()  # Close the connection if the pool is full

    @contextmanager
    def ftp_connection(self, auto_release=True):
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._release_connection(conn, auto_release)

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
    def upload_file(self, local_file_path, remote_file_path, progress_callback=None, auto_release=True):
        """
        Uploads a file to the FTP server with retry and progress tracking.

        :param local_file_path: Path to the local file to be uploaded.
        :param remote_file_path: Path on the remote server where the file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :param auto_release: Whether to release the FTP connection after the upload.
        :raises FTPTransferError: If the file upload fails after retries.
        """
        self.logger.info(f"Starting upload of {local_file_path} to {remote_file_path}")
        with self.ftp_connection(auto_release) as ftp:
            try:
                with open(local_file_path, 'rb') as file:
                    total_size = os.path.getsize(local_file_path)
                    # Display a progress bar if no callback is provided
                    if progress_callback is None:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Uploading") as pbar:
                            def progress_callback(block):
                                pbar.update(len(block))

                    ftp.storbinary(f"STOR {remote_file_path}", file, callback=progress_callback)

                self.logger.info(f"Uploaded: {local_file_path} to {remote_file_path}")
            except Exception as e:
                self.logger.warning(f"Retry attempt for file upload: {local_file_path}")
                raise FTPTransferError(f"Failed to upload file {local_file_path}: {e}")


    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def download_file(self, remote_file_path, local_file_path, progress_callback=None, auto_release=True):
        """
        Downloads a file from the FTP server with retry and progress tracking.

        :param remote_file_path: Path to the file on the FTP server.
        :param local_file_path: Local path where the downloaded file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :param auto_release: Whether to release the FTP connection after the download.
        :raises FTPTransferError: If the file download fails after retries.
        """
        self.logger.info(f"Starting download of {remote_file_path} to {local_file_path}")

        try:
            with self.ftp_connection(auto_release) as ftp:
                # Open the local file in write-binary mode
                with open(local_file_path, 'wb') as file:
                    total_size = ftp.size(remote_file_path)

                    # Default progress tracking using tqdm if no callback is provided
                    if progress_callback is None:
                        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                            def progress_callback(data):
                                file.write(data)
                                pbar.update(len(data))
                    else:
                        # Custom progress callback
                        def progress_callback(data):
                            file.write(data)
                            if progress_callback:
                                progress_callback(len(data))

                    # Download the file using RETR command
                    ftp.retrbinary(f"RETR {remote_file_path}", progress_callback)

                # Check if the file was actually downloaded
                local_file_size = os.path.getsize(local_file_path)
                if local_file_size == 0:
                    raise FTPTransferError(f"Downloaded file {local_file_path} is empty (0KB)")

                self.logger.info(f"Downloaded: {remote_file_path} to {local_file_path}")

        except ftplib.error_perm as e:
            if str(e).startswith("550"):
                self.logger.error(f"File not found on server: {remote_file_path}")
            raise FTPTransferError(f"Failed to download file {remote_file_path}: {e}")
        except Exception as e:
            raise FTPTransferError(f"Error during download: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def list_files(self, remote_path, only_files=True, auto_release=True):
        """
        Lists the files in a specified directory on the FTP server.

        :param remote_path: The directory path on the remote server.
        :param only_files: Whether to list only files (True) or include both files and directories (False).
        :param auto_release: Whether to release the FTP connection after the operation.
        :return: A list of filenames in the directory or an empty list if the directory is empty.
        :raises FTPTransferError: If listing files fails.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                files = ftp.nlst(remote_path)  # List files and directories in the directory
                if only_files:
                    files = [f for f in files if not self._is_directory(ftp, f)]  # Filter out directories
                return files
        except ftplib.error_perm as e:
            if str(e).startswith("550"):  # Handle empty directory
                self.logger.warning(f"Directory {remote_path} is empty or not accessible.")
                return []  # Return empty list for empty directory
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

    def directory_exists(self, remote_path, auto_release=True):
        """
        Checks if a directory exists on the FTP server.

        :param remote_path: The path of the remote directory to check.
        :param auto_release: Whether to release the FTP connection after the operation.
        :return: True if the directory exists, False otherwise.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.cwd(remote_path)
                return True
        except ftplib.error_perm:
            return False

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def move_file(self, src_remote_path, dest_remote_directory, auto_release=True):
        """
        Moves a file from one directory to another on the FTP server.

        :param src_remote_path: The source path of the file to be moved.
        :param dest_remote_directory: The destination directory where the file will be moved.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If moving the file fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                # Extract the file name from the source path
                file_name = os.path.basename(src_remote_path)

                # Ensure the destination directory exists
                if not self.directory_exists(dest_remote_directory, auto_release=False):
                    self.create_directory(dest_remote_directory, auto_release=False)
                    self.logger.info(f"Created directory: {dest_remote_directory}")

                # Construct the destination path
                dest_remote_path = os.path.join(dest_remote_directory, file_name)

                # Move (rename) the file
                ftp.rename(src_remote_path, dest_remote_path)
                self.logger.info(f"Moved file from {src_remote_path} to {dest_remote_path}")

        except Exception as e:
            raise FTPTransferError(f"Failed to move file from {src_remote_path} to {dest_remote_directory}: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def rename_file(self, old_remote_path, new_remote_path, auto_release=True):
        """
        Renames a file on the FTP server.

        :param old_remote_path: The current path of the remote file.
        :param new_remote_path: The new path for the remote file.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If the file renaming fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.rename(old_remote_path, new_remote_path)
                self.logger.info(f"Renamed file from {old_remote_path} to {new_remote_path}")
        except Exception as e:
            raise FTPTransferError(f"Failed to rename file from {old_remote_path} to {new_remote_path}: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def delete_file(self, remote_file_path, auto_release=True):
        """
        Deletes a file from the FTP server.

        :param remote_file_path: The path of the remote file to be deleted.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If the file deletion fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.delete(remote_file_path)
                self.logger.info(f"Deleted file: {remote_file_path}")
        except Exception as e:
            raise FTPTransferError(f"Failed to delete file {remote_file_path}: {e}")

    def check_file_exists(self, remote_file_path, auto_release=True):
        """
        Checks if a file exists on the FTP server.

        :param remote_file_path: The path of the remote file to check.
        :param auto_release: Whether to release the FTP connection after the operation.
        :return: True if the file exists, False otherwise.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                files = ftp.nlst(os.path.dirname(remote_file_path))  # List files in the directory
                return os.path.basename(remote_file_path) in files
        except Exception as e:
            self.logger.error(f"Failed to check if file exists {remote_file_path}: {e}")
            return False

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def create_directory(self, remote_directory_path, auto_release=True):
        """
        Creates a new directory on the FTP server.

        :param remote_directory_path: The path of the remote directory to be created.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If the directory creation fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.mkd(remote_directory_path)
                self.logger.info(f"Created directory: {remote_directory_path}")
        except Exception as e:
            raise FTPTransferError(f"Failed to create directory {remote_directory_path}: {e}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def remove_directory(self, remote_directory_path, auto_release=True):
        """
        Removes a directory from the FTP server.

        :param remote_directory_path: The path of the remote directory to be removed.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If the directory removal fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.rmd(remote_directory_path)
                self.logger.info(f"Removed directory: {remote_directory_path}")
        except Exception as e:
            raise FTPTransferError(f"Failed to remove directory {remote_directory_path}: {e}")

    def change_directory(self, remote_directory_path, auto_release=True):
        """
        Changes the current working directory on the FTP server.

        :param remote_directory_path: The path of the remote directory to change to.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If changing directory fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.cwd(remote_directory_path)
                self.logger.info(f"Changed directory to: {remote_directory_path}")
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

    def verify_file_integrity(self, local_file_path, remote_file_path, auto_release=True):
        """
        Verifies the integrity of a file by comparing local and remote checksums or sizes.

        :param local_file_path: The path of the local file.
        :param remote_file_path: The path of the remote file on the FTP server.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If verification fails.
        """
        local_checksum = self.calculate_md5(local_file_path)
        try:
            with self.ftp_connection(auto_release) as ftp:
                try:
                    # First, compare file sizes
                    local_size = os.path.getsize(local_file_path)
                    remote_size = ftp.size(remote_file_path)
                    if local_size != remote_size:
                        raise FTPTransferError(f"File size mismatch for {local_file_path} and {remote_file_path}")

                    # Then, try to fetch the MD5 checksum if supported by the server
                    remote_checksum = ftp.sendcmd(f'SITE MD5 {remote_file_path}')
                    remote_checksum = remote_checksum.split(' ')[1]  # Parse the response
                except Exception:
                    self.logger.warning("SITE MD5 command not supported. Downloading remote file for checksum comparison.")
                    remote_file_path_temp = f"{local_file_path}.temp"
                    self.download_file(remote_file_path, remote_file_path_temp)
                    remote_checksum = self.calculate_md5(remote_file_path_temp)
                    os.remove(remote_file_path_temp)  # Cleanup temporary file

                if local_checksum != remote_checksum:
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
