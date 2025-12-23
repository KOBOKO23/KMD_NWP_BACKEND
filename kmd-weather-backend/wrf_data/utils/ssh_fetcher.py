"""
SSH/SFTP Fetcher for WRF GRIB Files
Downloads GRIB files from remote WRF server, supports jump host
File: wrf_data/utils/ssh_fetcher.py
"""

from paramiko import SSHClient, AutoAddPolicy, RSAKey
from paramiko.proxy import ProxyCommand
import os
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional, Tuple
import xarray as xr

logger = logging.getLogger(__name__)

# GRIB parameters we want to extract
WANTED_PARAMS = ["tmax", "tmin", "2r", "cape", "tp"]  # max/min temp, RH, CAPE, precipitation

class WRFDataFetcher:
    """
    Fetches WRF model output files via SSH/SFTP with optional jump host
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
        timeout: int = 60
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

        self.timeout = timeout
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

            if self.jump_host:
                logger.info(f"Using jump host {self.jump_username}@{self.jump_host}:{self.jump_port}")
                if self.jump_key_path:
                    jump_key = RSAKey.from_private_key_file(self.jump_key_path, password=self.jump_key_password)
                    jump_cmd = f"ssh -i {self.jump_key_path} -p {self.jump_port} -W {self.host}:{self.port} {self.jump_username}@{self.jump_host}"
                else:
                    jump_cmd = f"ssh -p {self.jump_port} -W {self.host}:{self.port} {self.jump_username}@{self.jump_host}"
                sock = ProxyCommand(jump_cmd)

            if self.key_path:
                key = RSAKey.from_private_key_file(self.key_path, password=self.key_password)
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    pkey=key,
                    sock=sock,
                    timeout=self.timeout
                )
            else:
                self.ssh_client.connect(
                    hostname=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    sock=sock,
                    timeout=self.timeout
                )

            self.sftp_client = self.ssh_client.open_sftp()
            logger.info("✓ SSH connection established successfully")
            return True

        except Exception as e:
            logger.error(f"❌ Connection failed: {e}")
            raise

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
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def list_remote_files(self, remote_path: str, pattern: str = "wrfout_*") -> List[str]:
        if not self.sftp_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        logger.info(f"Listing files in {remote_path} matching {pattern}")
        
        try:
            files = self.sftp_client.listdir(remote_path)
            if pattern.endswith('*'):
                prefix = pattern[:-1]
                matching_files = [f for f in files if f.startswith(prefix)]
            else:
                matching_files = [f for f in files if pattern in f]
            return [os.path.join(remote_path, f) for f in matching_files]
        except Exception as e:
            logger.error(f"Error listing files in {remote_path}: {e}")
            return []

    def download_file(self, remote_path: str, local_path: str) -> bool:
        if not self.sftp_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Downloading: {remote_path} -> {local_path}")
        try:
            file_size = self.sftp_client.stat(remote_path).st_size
            self.sftp_client.get(remote_path, local_path)
            if os.path.exists(local_path) and os.path.getsize(local_path) == file_size:
                logger.info(f"✓ Downloaded successfully: {local_path}")
                return True
            else:
                logger.warning(f"⚠️ File size mismatch or missing: {local_path}")
                return False
        except Exception as e:
            logger.error(f"❌ Error downloading {remote_path}: {e}")
            return False

    def download_files(self, file_list: List[Dict[str, str]]) -> Dict[str, bool]:
        results = {}
        for f in file_list:
            results[f['remote']] = self.download_file(f['remote'], f['local'])
        logger.info(f"Downloaded {sum(results.values())}/{len(file_list)} files successfully")
        return results

    def get_wrf_files_for_date(
        self,
        date: datetime,
        domain_suffix: str,
        remote_base_path: str,
        local_base_path: str,
        hours: int = 72
    ) -> List[Dict[str, str]]:
        file_list = []
        for hour in range(0, hours + 1):
            forecast_time = date + timedelta(hours=hour)
            filename = f"WRFPRS_d{domain_suffix}.{hour:02d}"
            remote_file = os.path.join(remote_base_path, filename)
            local_file = os.path.join(local_base_path, date.strftime('%Y%m%d'), filename)
            file_list.append({'remote': remote_file, 'local': local_file, 'hour': hour, 'valid_time': forecast_time})
        return file_list

    def execute_command(self, command: str) -> Tuple[str, str, int]:
        if not self.ssh_client:
            raise ConnectionError("Not connected. Call connect() first.")
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            return stdout.read().decode('utf-8'), stderr.read().decode('utf-8'), exit_code
        except Exception as e:
            logger.error(f"❌ Error executing command: {e}")
            return "", str(e), -1

def fetch_wrf_data(date: datetime, domain: str, config: Dict) -> xr.Dataset:
    """
    Fetch WRF GRIB files and return xarray.Dataset with selected variables:
    2m max/min temp, RH, CAPE, precipitation
    """
    if domain == 'kenya':
        domain_suffix = '01'
        remote_path = config.get('WRF_REMOTE_GRIB_PATH')
    else:
        domain_suffix = '02'
        remote_path = config.get('WRF_REMOTE_GRIB_PATH')
    local_path = config.get('LOCAL_DATA_PATH')

    fetcher = WRFDataFetcher(
        host=config['WRF_TARGET_HOST'],
        port=config.get('WRF_TARGET_PORT', 22),
        username=config['WRF_TARGET_USERNAME'],
        password=config.get('WRF_TARGET_PASSWORD'),
        jump_host=config.get('WRF_JUMP_HOST'),
        jump_port=config.get('WRF_JUMP_PORT', 22),
        jump_username=config.get('WRF_JUMP_USERNAME'),
        jump_password=config.get('WRF_JUMP_PASSWORD'),
        timeout=60
    )

    downloaded_files = []
    with fetcher:
        file_list = fetcher.get_wrf_files_for_date(
            date=date,
            domain_suffix=domain_suffix,
            remote_base_path=remote_path,
            local_base_path=local_path,
            hours=config.get('FORECAST_HOURS', 72)
        )
        results = fetcher.download_files(file_list)
        for f, success in zip(file_list, results.values()):
            if success:
                downloaded_files.append(f['local'])

    if not downloaded_files:
        raise ValueError("No WRF files were downloaded successfully.")

    # Load only required variables using cfgrib
    datasets = []
    for f in downloaded_files:
        try:
            ds = xr.open_dataset(f, engine="cfgrib", filter_by_keys={"shortName": WANTED_PARAMS})
            datasets.append(ds)
        except Exception as e:
            logger.warning(f"Failed to open {f}: {e}")

    if not datasets:
        raise ValueError("No GRIB files could be opened.")

    combined = xr.concat(datasets, dim="time") if len(datasets) > 1 else datasets[0]

    # Unit conversions
    if "tmax" in combined:
        combined["tmax"] = combined["tmax"] - 273.15  # K -> °C
    if "tmin" in combined:
        combined["tmin"] = combined["tmin"] - 273.15  # K -> °C
    if "tp" in combined:
        combined["tp"] = combined["tp"] * 1000  # m -> mm

    return combined
