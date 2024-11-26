import os
import re
import stat
import ftplib
import hashlib
import paramiko
import fnmatch
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import Callable, List, Optional, Union, Pattern
from retrying import retry  # To handle retrying failed operations
import threading  # For thread-safe connection pool
import logging  # For logging errors and information
from tqdm import tqdm  # For tracking progress
from contextlib import contextmanager
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor  # For parallel file transfers

class TransferProtocol(Enum):
    """Enum for supported file transfer protocols"""
    FTP = "FTP"
    FTPS = "FTPS"
    SFTP = "SFTP"

class OverwriteAction(Enum):
    """Enum for file overwrite actions"""
    SKIP = "skip"  # Skip if file exists
    OVERWRITE = "overwrite"  # Always overwrite existing files
    RENAME = "rename"  # Rename new file if exists (append timestamp)

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

    def __init__(
        self,
        hostname: str,
        username: str,
        password: str,
        port: int = None,
        use_tls=False,
        protocol: TransferProtocol = TransferProtocol.FTP,
        max_connections: int = 5,
        timeout: int = 30,
        log_level: int = logging.INFO,
        retry_attempts: int = 5,
        retry_multiplier: int = 1000,
        retry_max: int = 10000,
        use_passive_mode: bool = True
    ):

        """
        Initializes the FTPClient with server credentials and connection settings.

        :param hostname: The FTP server hostname.
        :param username: FTP account username.
        :param password: FTP account password.
        :param protocol: Transfer protocol (FTP, FTPS, or SFTP)
        :param port: Server port (default: 21 for FTP/FTPS, 22 for SFTP)
        :param use_tls: Whether to use FTPS (TLS) or plain FTP. Default is False (use FTPS).
        :param max_connections: Maximum number of FTP connections to pool.
        :param timeout: Timeout for the FTP connections in seconds.
        :param log_level: Level of logging (default is INFO).
        """

        if max_connections <= 0:
            raise ValueError("max_connections must be positive")

        if timeout <= 0:
            raise ValueError("timeout must be positive")

        self.hostname = hostname
        self.username = username
        self.password = password

        # Set default ports based on protocol if not specified
        if port is None:
            self.port = 22 if protocol == TransferProtocol.SFTP else 21
        else:
            self.port = int(port)

        self.protocol = protocol
        self.use_tls = use_tls
        self.timeout = timeout
        self.use_passive_mode = use_passive_mode
        self.connection_pool = []  # Pool of reusable FTP connections
        self.max_connections = max_connections
        self.lock = threading.Lock()  # Ensures thread-safe access to the connection pool

        # Retry settings for FTP operations
        self.retry_attempts = retry_attempts
        self.retry_multiplier = retry_multiplier
        self.retry_max = retry_max

        # For SFTP connections
        self.ssh = None
        self.sftp = None

        # Set up logging to track client operations
        logging.basicConfig(level=log_level)
        self.logger = logging.getLogger(__name__)

    def _create_connection(self):
        """Creates new connection based on protocol type."""
        try:
            if self.protocol == TransferProtocol.SFTP:
                return self._create_sftp_connection()
            else:
                return self._create_ftp_connection()
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise FTPConnectionError(f"Connection failed: {str(e)}")

    def _create_sftp_connection(self):
        """Creates a new SFTP connection."""
        try:
            self.logger.info(f"Attempting SFTP connection to {self.hostname}:{self.port}")

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            ssh.connect(
                self.hostname,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=self.timeout
            )

            sftp = ssh.open_sftp()
            return {'ssh': ssh, 'sftp': sftp}

        except Exception as e:
            raise FTPConnectionError(f"SFTP connection failed: {str(e)}")

    def _create_ftp_connection(self):
        """Creates a new FTP or FTPS connection."""
        try:
            ftp = ftplib.FTP_TLS(timeout=self.timeout) if self.protocol == TransferProtocol.FTPS else ftplib.FTP(timeout=self.timeout)
            ftp.set_debuglevel(2)

            self.logger.info(f"Attempting FTP/FTPS connection to {self.hostname}:{self.port}")
            ftp.connect(self.hostname, self.port)

            if self.use_passive_mode:
                self.logger.info("Setting passive mode")
                ftp.set_pasv(True)

            ftp.login(self.username, self.password)

            if self.protocol == TransferProtocol.FTPS:
                self.logger.info("Setting up TLS protection")
                ftp.prot_p()

            return ftp

        except Exception as e:
            raise FTPConnectionError(f"FTP/FTPS connection failed: {str(e)}")

    def toggle_transfer_mode(self, use_passive_mode: bool) -> None:
        """
        Toggles between active and passive mode for data transfers.

        Args:
            use_passive_mode: True for passive mode, False for active mode
        """
        self.use_passive_mode = use_passive_mode

        # Update existing connections in the pool
        with self.lock:
            for conn in self.connection_pool:
                try:
                    conn.set_pasv(use_passive_mode)
                except Exception as e:
                    self.logger.warning(f"Failed to update transfer mode for connection: {e}")

    def _get_connection(self):
        """
        Retrieves an available connection from the pool or creates a new one if the pool is empty.

        :return: An FTP connection.
        """
        with self.lock:
            if self.protocol == TransferProtocol.SFTP:
                # For SFTP, use single connection
                if not self.connection_pool:
                    return self._create_connection()
                conn = self.connection_pool[0]
                try:
                    conn['sftp'].stat('.')  # Test connection
                    return conn
                except Exception:
                    self.logger.warning("Recreating dropped SFTP connection")
                    return self._create_connection()
            else:
                # For FTP/FTPS, use connection pool
                if self.connection_pool:
                    conn = self.connection_pool.pop()
                    try:
                        conn.voidcmd("NOOP")
                        return conn
                    except Exception:
                        self.logger.warning("Recreating dropped FTP connection")
                        return self._create_connection()
                return self._create_connection()

    def _release_connection(self, conn, auto_release=True):
        """
        Releases a connection back to the pool or closes it if the pool is full, based on auto_release.

        :param conn: The FTP connection to release.
        :param auto_release: Whether to release the connection back to the pool (default is True).
        """
        if auto_release:
            with self.lock:
                if self.protocol == TransferProtocol.SFTP:
                    # For SFTP, keep single connection
                    if not self.connection_pool:
                        self.connection_pool = [conn]
                else:
                    # For FTP/FTPS, manage connection pool
                    if len(self.connection_pool) < self.max_connections:
                        self.connection_pool.append(conn)
                    else:
                        self._close_connection(conn)

    def _close_connection(self, conn):
        """Closes a connection based on protocol type."""
        try:
            if self.protocol == TransferProtocol.SFTP:
                conn['sftp'].close()
                conn['ssh'].close()
            else:
                conn.quit()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")

    @contextmanager
    def ftp_connection(self, auto_release=True):
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._release_connection(conn, auto_release)

    @contextmanager
    def get_connection(self, auto_release=True):
        """Context manager for connections."""
        conn = self._get_connection()
        try:
            yield conn
        finally:
            self._release_connection(conn, auto_release)

    def connect(self):
        """
        Initialize connection based on protocol type.
        For SFTP, create single connection. For FTP/FTPS, pre-warm connection pool.
        """
        if self.protocol == TransferProtocol.SFTP:
            # For SFTP, maintain single connection
            conn = self._create_connection()
            self.connection_pool = [conn]
            self.logger.info("SFTP connection established")
        else:
            # For FTP/FTPS, pre-warm the connection pool
            for _ in range(self.max_connections):
                self.connection_pool.append(self._create_connection())
            self.logger.info(f"{len(self.connection_pool)} FTP connections established")

    def disconnect(self):
        """
        Close all connections in the pool when done.
        """
        while self.connection_pool:
            conn = self.connection_pool.pop()
            try:
                self._close_connection(conn)  # Close each connection in the pool
                self.logger.info("Connection closed successfully.")
            except (ftplib.error_temp, ConnectionResetError) as e:
                # Handle errors when the connection is already closed or reset
                self.logger.warning(f"Connection already closed or reset: {e}")
            except Exception as e:
                # Catch any other unexpected exceptions
                self.logger.error(f"Error while closing FTP connection: {e}")
        self.logger.info("Disconnected from FTP server.")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def download_file(self, remote_path, local_path, progress_callback=None, auto_release=True):
        """
        Downloads a file from the FTP server with retry and progress tracking.

        :param remote_path: Path to the file on the FTP server.
        :param local_path: Local path where the downloaded file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :param auto_release: Whether to release the FTP connection after the download.
        :raises FTPTransferError: If the file download fails after retries.
        """
        self.logger.info(f"Starting download of {remote_path} to {local_path}")

        try:

            # Get the file size for progress tracking
            with self.get_connection(auto_release) as conn:
                if self.protocol == TransferProtocol.SFTP:
                    file_size = conn['sftp'].stat(remote_path).st_size
                else:
                    file_size = conn.size(remote_path)

                # Open the local file in write-binary mode
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Downloading {os.path.basename(remote_path)}") as pbar:

                    # Default progress tracking using tqdm if no callback is provided
                    def update_progress(sent, total=None):
                        pbar.update(sent - pbar.n)

                    # Custom progress callback
                    if self.protocol == TransferProtocol.SFTP:
                        conn['sftp'].get(
                            remote_path,
                            local_path,
                            callback=update_progress if not progress_callback else progress_callback
                        )
                    else:
                        with open(local_path, 'wb') as file:
                            conn.retrbinary(
                                f"RETR {remote_path}",
                                lambda data: (file.write(data), update_progress(len(data)))[0]
                            )

            self.logger.info(f"Downloaded: {remote_path} to {local_path}")

        except Exception as e:
            raise FTPTransferError(f"Download failed: {str(e)}")

    def download_matching_files(
        self,
        local_directory: str,
        remote_directory: str,
        filemask: str = "*",
        overwrite_action: Union[OverwriteAction, str] = OverwriteAction.SKIP,
        logger: Optional[logging.Logger] = None
    ) -> List[str]:
        """
        Downloads files from FTP server matching specified criteria, similar to Talend's tFTPGet component.

        Args:
            ftp_client: Instance of FTPClient class
            local_directory: Local directory where files will be downloaded
            remote_directory: Remote FTP directory from which to download files
            filemask: Wildcard pattern or regex pattern for file filtering (default: "*")
            overwrite_action: Action to take when file exists (skip/overwrite/rename)
            logger: Optional logger instance for logging operations

        Returns:
            List[str]: List of successfully downloaded file paths

        Raises:
            ValueError: If invalid parameters are provided
            FTPTransferError: If FTP operations fail
        """
        # Set up logging
        if logger is None:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.INFO)

        # Validate and normalize inputs
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)

        local_directory = os.path.abspath(local_directory)

        # Convert string overwrite_action to enum if necessary
        if isinstance(overwrite_action, str):
            try:
                overwrite_action = OverwriteAction(overwrite_action.lower())
            except ValueError:
                raise ValueError(f"Invalid overwrite_action: {overwrite_action}. Must be one of: {[a.value for a in OverwriteAction]}")

        # Determine if filemask is regex or wildcard
        is_regex = any(c in filemask for c in "[]()+{}^$")
        if is_regex:
            try:
                pattern = re.compile(filemask)
            except re.error:
                raise ValueError(f"Invalid regex pattern: {filemask}")
        else:
            # Convert wildcard to regex
            pattern = re.compile(fnmatch.translate(filemask))

        downloaded_files = []

        try:
            # List all files in remote directory
            remote_files = self.list_files(remote_directory)

            # Filter files based on mask
            matching_files = [
                f for f in remote_files
                if pattern.match(os.path.basename(f))
            ]

            if not matching_files:
                logger.info(f"No files matching pattern '{filemask}' found in {remote_directory}")
                return downloaded_files

            # Process each matching file
            for remote_file in matching_files:
                remote_path = os.path.join(remote_directory, remote_file)
                local_path = os.path.join(local_directory, os.path.basename(remote_file))

                try:
                    # Check if local file exists
                    if os.path.exists(local_path):
                        if overwrite_action == OverwriteAction.SKIP:
                            logger.info(f"Skipping existing file: {local_path}")
                            continue
                        elif overwrite_action == OverwriteAction.RENAME:
                            # Generate new filename with timestamp
                            base, ext = os.path.splitext(local_path)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            local_path = f"{base}_{timestamp}{ext}"
                            logger.info(f"Renaming to avoid conflict: {local_path}")
                        # For OVERWRITE, we'll just proceed with the download

                    # Download the file
                    logger.info(f"Downloading {remote_path} to {local_path}")
                    self.download_file(remote_path, local_path)

                    # Verify the download
                    if self.check_file_exists(remote_path):
                        downloaded_files.append(local_path)
                        logger.info(f"Successfully downloaded: {local_path}")
                    else:
                        logger.error(f"Failed to verify download: {local_path}")

                except Exception as e:
                    logger.error(f"Error downloading {remote_path}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error during FTP GET operation: {str(e)}")
            raise

        return downloaded_files

    def periodic_keep_alive(self, ftp, interval=300):
        """
        Periodically sends NOOP to keep the FTP connection alive every `interval` seconds.
        """
        self.keep_alive(ftp)
        threading.Timer(interval, self.periodic_keep_alive, args=[ftp, interval]).start()

    @sleep_and_retry
    @limits(calls=100, period=60)  # 100 calls per minute
    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def upload_file(
        self,
        local_path: str,
        remote_path: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        auto_release: bool = True
    ) -> None:
        """
        Uploads a file to the FTP server with retry and progress tracking.

        :param local_path: Path to the local file to be uploaded.
        :param remote_path: Path on the remote server where the file will be stored.
        :param progress_callback: Optional callback for progress tracking.
        :param auto_release: Whether to release the FTP connection after the upload.
        :raises FTPTransferError: If the file upload fails after retries.
        """
        self.logger.info(f"Starting upload of {local_path} to {remote_path}")
        try:
            file_size = os.path.getsize(local_path)

            # Open the local file in read-binary mode
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Uploading {os.path.basename(local_path)}") as pbar:

                # Default progress tracking using tqdm if no callback is provided
                def update_progress(sent, total=None):
                    pbar.update(sent - pbar.n)
                # Custom progress callback
                with self.get_connection(auto_release) as conn:
                    if self.protocol == TransferProtocol.SFTP:
                        conn['sftp'].put(
                            local_path,
                            remote_path,
                            callback=update_progress if not progress_callback else progress_callback
                        )
                    else:
                        with open(local_path, 'rb') as file:
                            conn.storbinary(
                                f"STOR {remote_path}",
                                file,
                                callback=lambda block: update_progress(len(block))
                            )

            self.logger.info(f"Uploaded: {local_path} to {remote_path}")

        except Exception as e:
            raise FTPTransferError(f"Upload failed: {str(e)}")

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def list_files(self, remote_path: str, only_files: bool = True, auto_release: bool = True) -> List[str]:
        """
        Lists the files in a specified directory on the FTP server.

        :param remote_path: The directory path on the remote server.
        :param only_files: Whether to list only files (True) or include both files and directories (False).
        :param auto_release: Whether to release the FTP connection after the operation.
        :return: A list of filenames in the directory or an empty list if the directory is empty.
        :raises FTPTransferError: If listing files fails.

        Returns:
            List[str]: List of file names in the directory
        """
        try:
            with self.get_connection(auto_release) as conn:  # Changed from ftp_connection
                if self.protocol == TransferProtocol.SFTP:
                    files = conn['sftp'].listdir(remote_path) # List files in the directory
                    if only_files:
                        files = [f for f in files if not self._is_directory_sftp(conn['sftp'], f)] # Filter out directories
                else:
                    files = conn.nlst(remote_path) # List files in the directory
                    if only_files:
                        files = [f for f in files if not self._is_directory(conn, f)] # Filter out directories
                return files
        except Exception as e:
            self.logger.error(f"Failed to list files: {e}")
            return []

    def _is_directory_sftp(self, sftp, path):
        """Check if path is directory for SFTP connections."""
        try:
            return stat.S_ISDIR(sftp.stat(path).st_mode)
        except Exception:
            return False

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
            with self.get_connection(auto_release) as conn: # Changed from ftp_connection
                if self.protocol == TransferProtocol.SFTP:
                    try:
                        mode = conn['sftp'].stat(remote_path).st_mode # Get the file mode
                        return stat.S_ISDIR(mode)
                    except Exception:
                        return False
                else:
                    try:
                        conn.cwd(remote_path) # Try to change to the directory
                        return True
                    except ftplib.error_perm:
                        return False
        except Exception:
            return False

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def move_file(self, src_remote_path, dest_remote_directory, auto_release=True, overwrite=True):
        """
        Moves a file from one directory to another on the FTP server.

        :param src_remote_path: The source path of the file to be moved.
        :param dest_remote_directory: The destination directory where the file will be moved.
        :param auto_release: Whether to release the FTP connection after the operation.
        :param overwrite: Whether to overwrite the file if it already exists in the destination.
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

                # Check if file exists in destination
                dest_exists = self.check_file_exists(dest_remote_path, auto_release=False)

                if dest_exists:
                    if overwrite:
                        # Delete existing file if overwrite is True
                        try:
                            ftp.delete(dest_remote_path)
                            self.logger.info(f"Deleted existing file at destination: {dest_remote_path}")
                        except ftplib.error_perm as e:
                            raise FTPTransferError(f"Failed to delete existing file at {dest_remote_path}: {e}")
                    else:
                        # Generate a unique filename using timestamp
                        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                        file_base, file_ext = os.path.splitext(file_name)
                        new_file_name = f"{file_base}_{timestamp}{file_ext}"
                        dest_remote_path = os.path.join(dest_remote_directory, new_file_name)
                        self.logger.info(f"File exists at destination, using unique name: {new_file_name}")

                # Move (rename) the file
                ftp.rename(src_remote_path, dest_remote_path)
                self.logger.info(f"Moved file from {src_remote_path} to {dest_remote_path}")

        except ftplib.error_perm as e:
            error_msg = str(e)
            if "550" in error_msg:  # Handle specific FTP error codes
                if "already exists" in error_msg.lower():
                    raise FTPTransferError(f"Destination file already exists and overwrite is disabled: {dest_remote_path}")
                else:
                    raise FTPTransferError(f"Permission denied: {error_msg}")
            raise FTPTransferError(f"Failed to move file from {src_remote_path} to {dest_remote_directory}: {error_msg}")
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
    def delete_file(self, remote_path, auto_release=True):
        """
        Deletes a file from the FTP server.

        :param remote_path: The path of the remote file to be deleted.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If the file deletion fails after retries.
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                ftp.delete(remote_path)
                self.logger.info(f"Deleted file: {remote_path}")
        except Exception as e:
            raise FTPTransferError(f"Failed to delete file {remote_path}: {e}")

    def check_file_exists(self, remote_path, auto_release=True):
        """
        Checks if a file exists on the FTP server.

        Args:
            remote_path (str): The path of the remote file to check.
            auto_release (bool): Whether to release the FTP connection after the operation.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        try:
            with self.get_connection(auto_release) as conn:
                if self.protocol == TransferProtocol.SFTP: # Check if file exists for SFTP connections
                    try:
                        mode = conn['sftp'].stat(remote_path).st_mode # Get the file mode
                        return stat.S_ISREG(mode)
                    except Exception:
                        return False
                else:
                    try:
                        size = conn.size(remote_path)
                        return True
                    except:
                        try:
                            # Check if file exists using MLST command
                            conn.voidcmd(f"MLST {remote_path}")
                            return True
                        except:
                            return False
        except Exception as e:
            self.logger.error(f"Failed to check if file exists {remote_path}: {e}")
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

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=5)
    def remove_directory_recursive(self, remote_path: str, auto_release: bool = True) -> None:
        """
        Recursively removes a directory and all its contents from the FTP server.

        Args:
            remote_path: Path to the remote directory
            auto_release: Whether to release the FTP connection after operation

        Raises:
            FTPTransferError: If directory removal fails
        """
        try:
            with self.ftp_connection(auto_release) as ftp:
                # List all items in directory
                items = []
                ftp.retrlines(f'LIST {remote_path}', items.append)

                for item in items:
                    # Parse item details (permissions, name, etc.)
                    parts = item.split(None, 8)
                    if len(parts) < 9:
                        continue

                    item_name = parts[8]
                    full_path = f"{remote_path}/{item_name}"

                    # Check if item is directory (starts with 'd' in permissions)
                    if item[0] == 'd':
                        # Recursively remove subdirectory
                        self.remove_directory_recursive(full_path, auto_release=False)
                    else:
                        # Delete file
                        ftp.delete(full_path)
                        self.logger.info(f"Deleted file: {full_path}")

                # Remove the empty directory
                ftp.rmd(remote_path)
                self.logger.info(f"Removed directory: {remote_path}")

        except Exception as e:
            raise FTPTransferError(f"Failed to recursively remove directory {remote_path}: {e}")

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

    def verify_file_integrity(self, local_path, remote_path, auto_release=True):
        """
        Verifies the integrity of a file by comparing local and remote checksums or sizes.

        :param local_path: The path of the local file.
        :param remote_path: The path of the remote file on the FTP server.
        :param auto_release: Whether to release the FTP connection after the operation.
        :raises FTPTransferError: If verification fails.
        """
        local_checksum = self.calculate_md5(local_path)
        try:
            with self.ftp_connection(auto_release) as ftp:
                try:
                    # First, compare file sizes
                    local_size = os.path.getsize(local_path)
                    remote_size = ftp.size(remote_path)
                    if local_size != remote_size:
                        raise FTPTransferError(f"File size mismatch for {local_path} and {remote_path}")

                    # Then, try to fetch the MD5 checksum if supported by the server
                    remote_checksum = ftp.sendcmd(f'SITE MD5 {remote_path}')
                    remote_checksum = remote_checksum.split(' ')[1]  # Parse the response
                except Exception:
                    self.logger.warning("SITE MD5 command not supported. Downloading remote file for checksum comparison.")
                    remote_path_temp = f"{local_path}.temp"
                    self.download_file(remote_path, remote_path_temp)
                    remote_checksum = self.calculate_md5(remote_path_temp)
                    os.remove(remote_path_temp)  # Cleanup temporary file

                if local_checksum != remote_checksum:
                    raise FTPTransferError(f"Checksum mismatch for {local_path} and {remote_path}")
                self.logger.info(f"Checksum verified for {local_path} and {remote_path}")

        except Exception as e:
            raise FTPTransferError(f"Failed to verify integrity for {local_path}: {e}")

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

    def get_server_features(self) -> List[str]:
        """Get supported server features"""
        with self.ftp_connection() as ftp:
            return ftp.sendcmd('FEAT').split('\n')

    def __del__(self):
        """Ensure all connections are closed"""
        self.disconnect()
