"""
GRIB File Processor for WRF Data
Extracts weather parameters from GRIB files and converts to grid data
File: wrf_data/utils/grib_processor.py
"""

import os
import pygrib
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime, timedelta

from .color_mapper import get_mapper_for_parameter

logger = logging.getLogger(__name__)


class GRIBProcessor:
    """
    Processes GRIB files from WRF model output
    """
    
    # Mapping of parameter codes to GRIB variable names
    PARAMETER_MAPPING = {
        'rainfall': {
            'name': 'Total Precipitation',
            'shortName': 'tp',
            'typeOfLevel': 'surface',
            'level': 0,
        },
        'temp-max': {
            'name': '2 metre temperature',
            'shortName': 'tmax',
            'typeOfLevel': 'heightAboveGround',
            'level': 2,
            'aggregate': 'max',  # Get maximum value
        },
        'temp-min': {
            'name': '2 metre temperature',
            'shortName': 'tmin',
            'typeOfLevel': 'heightAboveGround',
            'level': 2,
            'aggregate': 'min',  # Get minimum value
        },
        'rh': {
            'name': '2 metre relative humidity',
            'shortName': '2r',
            'typeOfLevel': 'heightAboveGround',
            'level': 2,
        },
        'cape': {
            'name': 'Convective available potential energy',
            'shortName': 'cape',
            'typeOfLevel': 'surface',
            'level': 0,
        },
    }
    
    def __init__(self, grib_file_path: str):
        """
        Initialize GRIB processor
        
        Args:
            grib_file_path: Path to GRIB file
        """
        self.grib_file_path = grib_file_path
        self.grib_data = None
    
    def open(self):
        """Open the GRIB file"""
        try:
            self.grib_data = pygrib.open(self.grib_file_path)
            logger.info(f"Opened GRIB file: {self.grib_file_path}")
        except Exception as e:
            logger.error(f"Failed to open GRIB file {self.grib_file_path}: {e}")
            raise
    
    def close(self):
        """Close the GRIB file"""
        if self.grib_data:
            self.grib_data.close()
            self.grib_data = None
    
    def __enter__(self):
        """Context manager entry"""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
    
    def extract_parameter(
        self, 
        parameter_code: str,
        apply_color_mapping: bool = True
    ) -> Optional[Dict]:
        """
        Extract a specific parameter from the GRIB file
        
        Args:
            parameter_code: Parameter code ('rainfall', 'temp-max', etc.)
            apply_color_mapping: Whether to apply color mapping
            
        Returns:
            Dict with 'lats', 'lons', 'values', 'color_data', 'metadata'
        """
        if not self.grib_data:
            raise ValueError("GRIB file not opened. Call open() first.")
        
        param_config = self.PARAMETER_MAPPING.get(parameter_code)
        if not param_config:
            logger.error(f"Unknown parameter code: {parameter_code}")
            return None
        
        try:
            # Try to find the message by name
            # Note: GRIB variable names may vary depending on WRF configuration
            # You may need to adjust these based on your actual GRIB files
            
            messages = []
            for msg in self.grib_data:
                # Try multiple matching strategies
                if (hasattr(msg, 'shortName') and msg.shortName == param_config.get('shortName')) or \
                   (hasattr(msg, 'name') and param_config['name'] in msg.name):
                    messages.append(msg)
            
            if not messages:
                logger.warning(f"No messages found for parameter {parameter_code}")
                return None
            
            # Use the first matching message (or aggregate if needed)
            msg = messages[0]
            
            # Extract data
            values = msg.values
            lats, lons = msg.latlons()
            
            # Convert temperature from Kelvin to Celsius if needed
            if parameter_code in ['temp-max', 'temp-min']:
                if values.max() > 200:  # Likely in Kelvin
                    values = values - 273.15
            
            # Calculate statistics
            valid_values = values[~np.isnan(values)]
            metadata = {
                'min': float(np.min(valid_values)) if len(valid_values) > 0 else None,
                'max': float(np.max(valid_values)) if len(valid_values) > 0 else None,
                'mean': float(np.mean(valid_values)) if len(valid_values) > 0 else None,
                'units': getattr(msg, 'units', 'unknown'),
                'valid_time': msg.validDate if hasattr(msg, 'validDate') else None,
                'forecast_time': msg.forecastTime if hasattr(msg, 'forecastTime') else None,
            }
            
            result = {
                'lats': lats.tolist(),
                'lons': lons.tolist(),
                'values': values.tolist(),
                'metadata': metadata,
            }
            
            # Apply color mapping if requested
            if apply_color_mapping:
                mapper = get_mapper_for_parameter(parameter_code)
                color_data = mapper.map_grid(values)
                result['color_data'] = color_data
            
            logger.info(f"Extracted {parameter_code}: {metadata}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting parameter {parameter_code}: {e}")
            return None
    
    def extract_all_parameters(self) -> Dict[str, Dict]:
        """
        Extract all supported parameters from the GRIB file
        
        Returns:
            Dict mapping parameter codes to extracted data
        """
        results = {}
        
        for param_code in self.PARAMETER_MAPPING.keys():
            data = self.extract_parameter(param_code)
            if data:
                results[param_code] = data
        
        return results
    
    def list_available_messages(self) -> List[Dict]:
        """
        List all available messages in the GRIB file
        Useful for debugging and understanding the file structure
        
        Returns:
            List of dicts with message information
        """
        if not self.grib_data:
            raise ValueError("GRIB file not opened. Call open() first.")
        
        messages = []
        
        for i, msg in enumerate(self.grib_data):
            info = {
                'index': i,
                'name': msg.name if hasattr(msg, 'name') else 'Unknown',
                'shortName': msg.shortName if hasattr(msg, 'shortName') else 'Unknown',
                'units': msg.units if hasattr(msg, 'units') else 'Unknown',
                'level': msg.level if hasattr(msg, 'level') else 'Unknown',
                'typeOfLevel': msg.typeOfLevel if hasattr(msg, 'typeOfLevel') else 'Unknown',
            }
            messages.append(info)
        
        return messages

def _process_grib_file(
    file_path: str,
    domain: str,
    parameters: Optional[List[str]] = None,
    subsample_factor: int = 4,
    apply_color_mapping: bool = True,
    previous_step_data: Optional[Dict[str, np.ndarray]] = None
) -> Dict:
    """
    Internal helper: process a single GRIB file, applying cumulative/running aggregation.
    previous_step_data: store previous step's cumulative/running values
    """
    if parameters is None:
        parameters = list(GRIBProcessor.PARAMETER_MAPPING.keys())

    if previous_step_data is None:
        previous_step_data = {}

    logger.info(f"Processing {file_path} for domain {domain}")
    result = {
        'file_path': file_path,
        'domain': domain,
        'parameters': {}
    }

    try:
        with GRIBProcessor(file_path) as processor:
            for param in parameters:
                data = processor.extract_parameter(param, apply_color_mapping=apply_color_mapping)
                if not data:
                    continue

                values = np.array(data['values'])

                # Initialize previous step values if not present
                if param not in previous_step_data:
                    previous_step_data[param] = np.zeros_like(values) if param == 'rainfall' else values

                # Apply parameter-specific aggregation
                if param == 'rainfall':
                    values = previous_step_data[param] + values
                elif param == 'temp-max':
                    values = np.maximum(previous_step_data[param], values)
                elif param == 'temp-min':
                    values = np.minimum(previous_step_data[param], values)
                elif param == 'cape':
                    values = np.maximum(previous_step_data.get(param, values), values)
                # RH stays instantaneous

                # Update previous_step_data for next step
                previous_step_data[param] = values

                # Subsample for frontend efficiency
                lats, lons = np.array(data['lats']), np.array(data['lons'])
                lats_sub, lons_sub, values_sub = lats[::subsample_factor, ::subsample_factor], \
                                                 lons[::subsample_factor, ::subsample_factor], \
                                                 values[::subsample_factor, ::subsample_factor]

                data['lats'] = lats_sub.tolist()
                data['lons'] = lons_sub.tolist()
                data['values'] = values_sub.tolist()

                # Update color mapping if applied
                if apply_color_mapping:
                    mapper = get_mapper_for_parameter(param)
                    data['color_data'] = mapper.map_grid(values)

                result['parameters'][param] = data

    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}")

    return result


def process_wrf_file(
    file_path: str,
    domain: str,
    parameters: Optional[List[str]] = None,
    subsample_factor: int = 4,
    apply_color_mapping: bool = True
) -> Dict:
    """
    Public function to process a single WRF GRIB file.
    """
    valid_time_str = file_path.split('_')[-1]  # e.g., wrfout_2025121809
    try:
        valid_time = datetime.strptime(valid_time_str, '%Y%m%d%H')
    except Exception:
        valid_time = None

    result = _process_grib_file(
        file_path=file_path,
        domain=domain,
        parameters=parameters,
        subsample_factor=subsample_factor,
        apply_color_mapping=apply_color_mapping
    )
    result['valid_time'] = valid_time.isoformat() if valid_time else None
    return result


def process_grib_folder(
    folder_path: str,
    domain: str,
    parameters: Optional[List[str]] = None,
    subsample_factor: int = 4,
    apply_color_mapping: bool = True
) -> List[Dict]:
    """
    Public function to batch process all GRIB files in a folder.
    """
    results = []
    for file in os.listdir(folder_path):
        if not file.startswith('wrfout_'):
            continue
        file_path = os.path.join(folder_path, file)
        results.append(_process_grib_file(
            file_path=file_path,
            domain=domain,
            parameters=parameters,
            subsample_factor=subsample_factor,
            apply_color_mapping=apply_color_mapping
        ))
    return results
