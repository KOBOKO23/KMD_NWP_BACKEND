"""
SSH/SFTP Fetcher for WRF GRIB Files - Production Version
Supports SSH key authentication (secure, no passwords in env vars)
File: wrf_data/utils/ssh_fetcher.py
"""

import paramiko
import os
import base64
import io
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Optional
from pathlib import Path
import time

logger = logging.getLogger(__name__)


class WRFDataFetcher:
    """
    Fetches WRF GRIB files via SSH with proxy jump server
    Supports SSH key authentication for security
    
    Connection flow:
    1. Connect to proxy server (nwp1@196.202.217.197:10002) - with SSH key
    2. Through proxy, connect to WRF server (nwp@192.168.1.201) - with SSH key
    3. Navigate to /home/nwp/DA/SEVERE/[YYYYMMDDHH]/
    4. Download WRFPRS_d01.XX (Kenya) and WRFPRS_d02.XX (East Africa) files
    """
    
    def __init__(
        self,
        # ===== REQUIRED: Proxy server =====
        proxy_host: str,
        proxy_port: int,
        proxy_username: str,

        # ===== REQUIRED: Target server =====
        target_host: str,
        target_username: str,

        # ===== OPTIONAL: Proxy auth =====
        proxy_password: Optional[str] = None,
        proxy_key_data: Optional[str] = None,

        # ===== OPTIONAL: Target auth =====
        target_password: Optional[str] = None,
        target_key_data: Optional[str] = None,
        target_port: int = 22,

        # ===== Paths & settings =====
        remote_archive_path: str = '/home/nwp/DA/SEVERE',
        local_data_path: str = 'data/raw',
        timeout: int = 60
    ):
    
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.proxy_key_data = proxy_key_data
        
        self.target_host = target_host
        self.target_port = target_port
        self.target_username = target_username
        self.target_password = target_password
        self.target_key_data = target_key_data
        
        self.remote_archive_path = remote_archive_path
        self.local_data_path = Path(local_data_path)
        self.timeout = timeout
        
        self.proxy_client = None
        self.target_client = None
        self.sftp_client = None
        self.proxy_transport = None
    
    def _load_private_key(self, key_data: str) -> paramiko.RSAKey:
        """
        Load SSH private key from string (supports base64 or PEM format)
        """
        try:
            # Try base64 decode first
            if not key_data.startswith('-----BEGIN'):
                try:
                    decoded = base64.b64decode(key_data)
                    key_data = decoded.decode('utf-8')
                except Exception:
                    pass  # Not base64, use as-is
            
            # Load key from string
            key_file = io.StringIO(key_data)
            
            # Try different key types
            for key_class in [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.DSSKey]:
                try:
                    key_file.seek(0)
                    return key_class.from_private_key(key_file)
                except Exception:
                    continue
            
            raise ValueError("Unsupported key format")
            
        except Exception as e:
            logger.error(f"Failed to load SSH private key: {e}")
            raise ValueError(f"Invalid SSH private key: {e}")
    
    def connect(self) -> bool:
        """
        Establish SSH connection through proxy to target server
        Uses SSH keys for authentication (secure, no passwords)
        """
        try:
            # Step 1: Connect to proxy server
            logger.info(f"🔗 Connecting to proxy server {self.proxy_username}@{self.proxy_host}:{self.proxy_port}...")
            self.proxy_client = paramiko.SSHClient()
            self.proxy_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Determine authentication method for proxy
            proxy_auth_kwargs = {
                'hostname': self.proxy_host,
                'port': self.proxy_port,
                'username': self.proxy_username,
                'timeout': self.timeout,
                'banner_timeout': self.timeout,
                'look_for_keys': False,  # Don't search default locations
            }
            
            if self.proxy_key_data:
                logger.info("  Using SSH key for proxy authentication")
                proxy_key = self._load_private_key(self.proxy_key_data)
                proxy_auth_kwargs['pkey'] = proxy_key
            elif self.proxy_password:
                logger.info("  Using password for proxy authentication")
                proxy_auth_kwargs['password'] = self.proxy_password
            else:
                raise ValueError("No authentication method provided for proxy server")
            
            self.proxy_client.connect(**proxy_auth_kwargs)
            logger.info("✓ Proxy connection established")
            
            # Step 2: Create transport channel through proxy
            self.proxy_transport = self.proxy_client.get_transport()
            
            # Open channel to target server through proxy
            dest_addr = (self.target_host, self.target_port)
            src_addr = (self.proxy_host, self.proxy_port)
            
            logger.info(f"🔗 Opening channel to target server {self.target_username}@{self.target_host}:{self.target_port}...")
            proxy_channel = self.proxy_transport.open_channel(
                "direct-tcpip",
                dest_addr,
                src_addr,
                timeout=self.timeout
            )
            
            # Step 3: Connect to target server through proxy channel
            self.target_client = paramiko.SSHClient()
            self.target_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Determine authentication method for target
            target_auth_kwargs = {
                'hostname': self.target_host,
                'port': self.target_port,
                'username': self.target_username,
                'sock': proxy_channel,
                'timeout': self.timeout,
                'banner_timeout': self.timeout,
                'look_for_keys': False,
            }
            
            if self.target_key_data:
                logger.info("  Using SSH key for target authentication")
                target_key = self._load_private_key(self.target_key_data)
                target_auth_kwargs['pkey'] = target_key
            elif self.target_password:
                logger.info("  Using password for target authentication")
                target_auth_kwargs['password'] = self.target_password
            else:
                raise ValueError("No authentication method provided for target server")
            
            self.target_client.connect(**target_auth_kwargs)
            logger.info("✓ Target server connection established")
            
            # Step 4: Open SFTP session
            self.sftp_client = self.target_client.open_sftp()
            logger.info("✓ SFTP session opened")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection failed: {e}", exc_info=True)
            self.disconnect()
            return False
    
    def disconnect(self):
        """Close all connections"""
        try:
            if self.sftp_client:
                self.sftp_client.close()
                self.sftp_client = None
                logger.info("SFTP session closed")
            
            if self.target_client:
                self.target_client.close()
                self.target_client = None
                logger.info("Target server connection closed")
            
            if self.proxy_client:
                self.proxy_client.close()
                self.proxy_client = None
                logger.info("Proxy connection closed")
                
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")
    
    def __enter__(self):
        if not self.connect():
            raise ConnectionError("Failed to establish SSH connection")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    def get_forecast_folder_name(self, run_date: datetime) -> str:
        """
        Generate folder name for a forecast run
        Format: YYYYMMDDHH (e.g., 2025011219 for Jan 12, 2025 at 19:00)
        
        WRF runs at 7pm (19:00) local time
        """
        return run_date.strftime('%Y%m%d%H')
    
    def list_available_runs(self) -> List[str]:
        """
        List all available forecast runs in the archive
        """
        if not self.sftp_client:
            raise ConnectionError("Not connected. Call connect() first.")
        
        try:
            logger.info(f"Listing folders in {self.remote_archive_path}")
            folders = self.sftp_client.listdir(self.remote_archive_path)
            
            # Filter for valid date folders (10 digits: YYYYMMDDHH)
            valid_folders = [f for f in folders if len(f) == 10 and f.isdigit()]
            valid_folders.sort(reverse=True)  # Most recent first
            
            logger.info(f"Found {len(valid_folders)} forecast runs")
            return valid_folders
            
        except Exception as e:
            logger.error(f"Error listing runs: {e}")
            return []
    
    def get_latest_run_folder(self) -> Optional[str]:
        """Get the most recent forecast run folder"""
        runs = self.list_available_runs()
        return runs[0] if runs else None
    
    def check_run_exists(self, run_date: datetime) -> bool:
        """
        Check if a forecast run exists for the given date
        """
        folder_name = self.get_forecast_folder_name(run_date)
        remote_path = f"{self.remote_archive_path}/{folder_name}"
        
        try:
            self.sftp_client.stat(remote_path)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.error(f"Error checking run existence: {e}")
            return False
    
    def list_grib_files(self, run_date: datetime, domain: str = 'both') -> List[Dict]:
        """
        List GRIB files for a specific run and domain
        
        Args:
            run_date: Forecast run datetime
            domain: 'kenya' (d01), 'east-africa' (d02), or 'both'
        
        Returns:
            List of dicts with file info: {name, remote_path, hour, domain}
        """
        if not self.sftp_client:
            raise ConnectionError("Not connected")
        
        folder_name = self.get_forecast_folder_name(run_date)
        remote_folder = f"{self.remote_archive_path}/{folder_name}"
        
        try:
            files = self.sftp_client.listdir(remote_folder)
            
            grib_files = []
            
            for filename in files:
                # Parse WRFPRS_d01.XX or WRFPRS_d02.XX
                if filename.startswith('WRFPRS_d'):
                    parts = filename.split('.')
                    if len(parts) == 2:
                        domain_part = parts[0]  # WRFPRS_d01 or WRFPRS_d02
                        hour_part = parts[1]    # 00, 01, 02, ..., 72
                        
                        domain_suffix = domain_part[-2:]  # '01' or '02'
                        domain_name = 'kenya' if domain_suffix == '01' else 'east-africa'
                        
                        # Filter by domain if specified
                        if domain == 'both' or \
                           (domain == 'kenya' and domain_suffix == '01') or \
                           (domain == 'east-africa' and domain_suffix == '02'):
                            
                            try:
                                hour = int(hour_part)
                                grib_files.append({
                                    'name': filename,
                                    'remote_path': f"{remote_folder}/{filename}",
                                    'hour': hour,
                                    'domain': domain_name,
                                    'domain_suffix': domain_suffix
                                })
                            except ValueError:
                                continue
            
            grib_files.sort(key=lambda x: (x['domain_suffix'], x['hour']))
            logger.info(f"Found {len(grib_files)} GRIB files for {folder_name}")
            return grib_files
            
        except Exception as e:
            logger.error(f"Error listing GRIB files: {e}")
            return []
    
    def download_file(self, remote_path: str, local_path: str, max_retries: int = 3) -> bool:
        """
        Download a single file with retry logic
        """
        if not self.sftp_client:
            raise ConnectionError("Not connected")
        
        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        for attempt in range(max_retries):
            try:
                logger.info(f"📥 Downloading: {os.path.basename(remote_path)} (attempt {attempt + 1}/{max_retries})")
                
                # Get file size
                file_size = self.sftp_client.stat(remote_path).st_size
                file_size_mb = file_size / (1024 * 1024)
                
                start_time = time.time()
                self.sftp_client.get(remote_path, local_path)
                elapsed = time.time() - start_time
                
                # Verify download
                if os.path.exists(local_path) and os.path.getsize(local_path) == file_size:
                    speed_mbps = file_size_mb / elapsed if elapsed > 0 else 0
                    logger.info(f"✓ Downloaded {file_size_mb:.2f}MB in {elapsed:.1f}s ({speed_mbps:.2f}MB/s)")
                    return True
                else:
                    logger.warning(f"⚠️ File size mismatch for {local_path}")
                    if os.path.exists(local_path):
                        os.remove(local_path)
                    
            except Exception as e:
                logger.error(f"❌ Download attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
        
        return False
    
    def download_forecast_run(
        self,
        run_date: datetime,
        domain: str = 'both',
        max_hours: int = 72
    ) -> Dict[str, List[str]]:
        """
        Download all GRIB files for a forecast run
        
        Args:
            run_date: Forecast run datetime
            domain: 'kenya', 'east-africa', or 'both'
            max_hours: Maximum forecast hours to download (0-72)
        
        Returns:
            Dict with 'success' and 'failed' lists of file paths
        """
        if not self.sftp_client:
            if not self.connect():
                raise ConnectionError("Failed to connect")
        
        folder_name = self.get_forecast_folder_name(run_date)
        local_folder = self.local_data_path / folder_name
        local_folder.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📦 Starting download for run: {folder_name}")
        logger.info(f"   Domain: {domain}, Max hours: {max_hours}")
        
        # Get list of files
        grib_files = self.list_grib_files(run_date, domain)
        
        # Filter by max_hours
        grib_files = [f for f in grib_files if f['hour'] <= max_hours]
        
        results = {
            'success': [],
            'failed': [],
            'total': len(grib_files),
            'run_folder': folder_name
        }
        
        logger.info(f"   Total files to download: {len(grib_files)}")
        
        for i, file_info in enumerate(grib_files, 1):
            remote_path = file_info['remote_path']
            local_path = str(local_folder / file_info['name'])
            
            logger.info(f"   [{i}/{len(grib_files)}] {file_info['name']}")
            
            # Skip if already downloaded
            if os.path.exists(local_path):
                try:
                    remote_size = self.sftp_client.stat(remote_path).st_size
                    local_size = os.path.getsize(local_path)
                    
                    if local_size == remote_size:
                        logger.info(f"   ↪ Already exists, skipping")
                        results['success'].append(local_path)
                        continue
                except Exception:
                    pass
            
            # Download
            if self.download_file(remote_path, local_path):
                results['success'].append(local_path)
            else:
                results['failed'].append(remote_path)
        
        logger.info(f"✓ Download complete: {len(results['success'])}/{results['total']} successful")
        
        return results


def create_fetcher_from_config(config: Dict) -> WRFDataFetcher:
    """
    Create WRFDataFetcher from Django settings config
    Supports both password and SSH key authentication
    """
    return WRFDataFetcher(
        proxy_host=config['JUMP_HOST'],
        proxy_port=config['JUMP_PORT'],
        proxy_username=config['JUMP_USERNAME'],
        proxy_password=config.get('JUMP_PASSWORD'),
        proxy_key_data=config.get('JUMP_SSH_KEY'),
        target_host=config['SSH_HOST'],
        target_port=config['SSH_PORT'],
        target_username=config['SSH_USERNAME'],
        target_password=config.get('SSH_PASSWORD'),
        target_key_data=config.get('SSH_PRIVATE_KEY'),
        remote_archive_path=config.get('REMOTE_BASE_PATH', '/home/nwp/DA/SEVERE'),
        local_data_path=config.get('LOCAL_DATA_PATH', 'data/raw'),
        timeout=60
    )