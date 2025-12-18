"""
SSH/SFTP Fetcher for WRF GRIB Files
Downloads GRIB files from remote WRF server
File: wrf_data/utils/ssh_fetcher.py
"""

from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.proxy import ProxyCommand
from scp import SCPClient
import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class WRFDataFetcher:
    """
    Fetches WRF model output files via SSH/SFTP with optional jump host support
    """
    def __init__(
        self,
        host: str,
        username: str,
        password: Optional[str] = None,
        key_path: Optional[str] = None,
        key_password: Optional[str] = None,
        port: int = 22,
        jump_host: Optional[str] = None,
        jump_port: int = 22,
        jump_username: Optional[str] = None,
        jump_password: Optional[str] = None,
        jump_key_path: Optional[str] = None,
        jump_key_password: Optional[str] = None,
    ):
        self.host = host
        self.username = username
        self.password = password
        self.key_path = key_path
        self.key_password = key_password
        self.port = port

        self.jump_host = jump_host
        self.jump_port = jump_port
        self.jump_username = jump_username
        self.jump_password = jump_password
        self.jump_key_path = jump_key_path
        self.jump_key_password = jump_key_password

        self.ssh_client = None
        self.sftp_client = None

    def connect(self):
        """
        Connect to the target server, optionally via jump host
        """
        logger.info(f"Connecting to {self.username}@{self.host}:{self.port}...")

        try:
            self.ssh_client = SSHClient()
            self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())

            sock = None
            # Setup jump host proxy if provided
            if self.jump_host:
                logger.info(f"Using jump host {self.jump_username}@{self.jump_host}:{self.jump_port}")
                if self.jump_key_path:
                    jump_key = RSAKey.from_private_key_file(self.jump_key_path, password=self.jump_key_password)
                    jump_cmd = f"ssh -i {self.jump_key_path} -W {self.host}:{self.port} {self.jump_username}@{self.jump_host}"
                else:
                    jump_cmd = f"ssh -W {self.host}:{self.port} {self.jump_username}@{self.jump_host}"
                sock = ProxyCommand(jump_cmd)

            # Connect to target server
            if self.key_path:
                key = RSAKey.from_private_key_file(self.key_path, password=self.key_password)
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    pkey=key,
                    sock=sock,
                    timeout=30
                )
            else:
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    sock=sock,
                    timeout=30
                )

            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("✓ SSH connection established successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise

    # ... keep all other methods as-is (list_remote_files, download_file, download_files, etc.)

    
    def disconnect(self):
        """Close SSH connection"""
        if self.sftp_client:
            self.sftp_client.close()
            self.sftp_client = None
        
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None
        
        logger.info("SSH connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def list_remote_files(self, remote_path: str, pattern: str = "wrfout_*") -> List[str]:
        """
        List files in remote directory matching pattern
        
        Args:
            remote_path: Remote directory path
            pattern: File pattern to match
            
        Returns:
            List of matching file paths
        """
        if not self.sftp_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        logger.info(f"Listing files in {remote_path} matching {pattern}")
        
        try:
            files = self.sftp_client.listdir(remote_path)
            
            # Filter by pattern (simple wildcard matching)
            if pattern.endswith('*'):
                prefix = pattern[:-1]
                matching_files = [f for f in files if f.startswith(prefix)]
            else:
                matching_files = [f for f in files if pattern in f]
            
            full_paths = [os.path.join(remote_path, f) for f in matching_files]
            
            logger.info(f"Found {len(matching_files)} matching files")
            return full_paths
            
        except Exception as e:
            logger.error(f"Error listing files in {remote_path}: {e}")
            return []
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """
        Download a single file from remote server
        
        Args:
            remote_path: Path to file on remote server
            local_path: Path where file should be saved locally
            
        Returns:
            True if successful, False otherwise
        """
        if not self.sftp_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        # Create local directory if it doesn't exist
        local_dir = os.path.dirname(local_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        
        logger.info(f"Downloading: {remote_path} -> {local_path}")
        
        try:
            # Get file size for progress tracking
            file_size = self.sftp_client.stat(remote_path).st_size
            logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
            
            # Download file
            self.sftp_client.get(remote_path, local_path)
            
            # Verify download
            if os.path.exists(local_path):
                local_size = os.path.getsize(local_path)
                if local_size == file_size:
                    logger.info(f"✓ Downloaded successfully: {local_path}")
                    return True
                else:
                    logger.warning(f"⚠️ File size mismatch: {local_size} != {file_size}")
                    return False
            else:
                logger.error("❌ Download failed: file not found locally")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error downloading {remote_path}: {e}")
            return False
    
    def download_files(self, file_list: List[Dict[str, str]]) -> Dict[str, bool]:
        """
        Download multiple files
        
        Args:
            file_list: List of dicts with 'remote' and 'local' keys
            
        Returns:
            Dict mapping remote paths to success status
        """
        results = {}
        
        for file_info in file_list:
            remote = file_info['remote']
            local = file_info['local']
            success = self.download_file(remote, local)
            results[remote] = success
        
        successful = sum(1 for v in results.values() if v)
        logger.info(f"Downloaded {successful}/{len(file_list)} files successfully")
        
        return results
    
    def get_wrf_files_for_date(
        self,
        date: datetime,
        domain_suffix: str,
        remote_base_path: str,
        local_base_path: str,
        hours: int = 72,
    ) -> List[Dict[str, str]]:
        """
        Get list of WRF files for a specific date and domain
        
        Args:
            date: Date to fetch (should be model run date)
            domain_suffix: '01' for Kenya, '02' for East Africa
            remote_base_path: Base path on remote server
            local_base_path: Base path for local storage
            hours: Number of forecast hours to fetch
            
        Returns:
            List of file info dicts with 'remote' and 'local' paths
        """
        file_list = []
        
        # WRF output files are typically named: wrfout_YYYYMMDDHH01 or wrfout_YYYYMMDDHH02
        # They're output hourly for the forecast period
        
        for hour in range(0, hours + 1):
            forecast_time = date + timedelta(hours=hour)
            
            # Format: wrfout_YYYYMMDDHH01 or wrfout_YYYYMMDDHH02
            filename = f"wrfout_{forecast_time.strftime('%Y%m%d%H')}{domain_suffix}"
            
            remote_file = os.path.join(remote_base_path, filename)
            local_file = os.path.join(
                local_base_path,
                date.strftime('%Y%m%d'),
                filename
            )
            
            file_list.append({
                'remote': remote_file,
                'local': local_file,
                'hour': hour,
                'valid_time': forecast_time,
            })
        
        logger.info(f"Generated file list: {len(file_list)} files for {date} (domain {domain_suffix})")
        return file_list
    
    def execute_command(self, command: str) -> Tuple[str, str, int]:
        """
        Execute a command on the remote server
        
        Args:
            command: Command to execute
            
        Returns:
            Tuple of (stdout, stderr, exit_code)
        """
        if not self.ssh_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        logger.info(f"Executing command: {command}")
        
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(command)
            
            stdout_text = stdout.read().decode('utf-8')
            stderr_text = stderr.read().decode('utf-8')
            exit_code = stdout.channel.recv_exit_status()
            
            if exit_code == 0:
                logger.info("✓ Command executed successfully")
            else:
                logger.warning(f"⚠️ Command exited with code {exit_code}")
            
            return stdout_text, stderr_text, exit_code
            
        except Exception as e:
            logger.error(f"❌ Error executing command: {e}")
            return "", str(e), -1


# Convenience function for quick file fetching
def fetch_wrf_data(
    date: datetime,
    domain: str,  # 'kenya' or 'east-africa'
    config: Dict,
) -> List[str]:
    """
    Fetch WRF data files for a specific date and domain
    
    Args:
        date: Model run date
        domain: 'kenya' or 'east-africa'
        config: Configuration dict with SSH and path settings
        
    Returns:
        List of downloaded file paths
    
    Example:
        from django.conf import settings
        
        files = fetch_wrf_data(
            date=datetime(2024, 12, 15, 9, 0),
            domain='kenya',
            config=settings.WRF_CONFIG
        )
    """
    # Determine domain suffix and paths
    if domain == 'kenya':
        domain_suffix = config.get('KENYA_FILE_SUFFIX', '01')
        remote_path = config.get('KENYA_PATH', '/data/wrf/kenya')
    else:  # east-africa
        domain_suffix = config.get('EAST_AFRICA_FILE_SUFFIX', '02')
        remote_path = config.get('EAST_AFRICA_PATH', '/data/wrf/east_africa')
    
    local_path = config.get('LOCAL_DATA_PATH')
    
    # Create fetcher instance
    fetcher = WRFDataFetcher(
        host=config['SSH_HOST'],
        username=config['SSH_USERNAME'],
        password=config.get('SSH_PASSWORD'),
        key_path=config.get('SSH_KEY_PATH'),
        key_password=config.get('SSH_KEY_PASSWORD'),
        port=config.get('SSH_PORT', 22),
    )
    
    downloaded_files = []
    
    try:
        with fetcher:
            # Get file list
            file_list = fetcher.get_wrf_files_for_date(
                date=date,
                domain_suffix=domain_suffix,
                remote_base_path=remote_path,
                local_base_path=local_path,
                hours=config.get('FORECAST_HOURS', 72),
            )
            
            # Download files
            results = fetcher.download_files(file_list)
            
            # Collect successfully downloaded files
            for file_info, success in zip(file_list, results.values()):
                if success:
                    downloaded_files.append(file_info['local'])
        
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Failed to fetch WRF data: {e}")
        raise
