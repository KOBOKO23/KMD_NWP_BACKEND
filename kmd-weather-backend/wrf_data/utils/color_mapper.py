"""
Color Mapper Utility
Maps parameter values to color scales defined in frontend
File: wrf_data/utils/color_mapper.py
"""

import numpy as np
from typing import List, Dict, Any


class ColorMapper:
    """
    Maps numerical weather data to color values based on defined color scales
    """
    
    def __init__(self, color_scale: List[Dict[str, Any]]):
        """
        Initialize with a color scale configuration
        
        Args:
            color_scale: List of dicts with 'min', 'max', 'color' keys
                        Example: [{'min': 0, 'max': 10, 'color': '#ffffff'}, ...]
        """
        self.color_scale = sorted(color_scale, key=lambda x: x['min'])
    
    def map_value(self, value: float) -> str:
        """
        Map a single value to its corresponding color
        
        Args:
            value: Numerical value to map
            
        Returns:
            Hex color string (e.g., '#ffffff')
        """
        if np.isnan(value) or value is None:
            return '#cccccc'  # Gray for missing data
        
        # Find the appropriate color range
        for scale_item in self.color_scale:
            if scale_item['min'] <= value < scale_item['max']:
                return scale_item['color']
        
        # If value exceeds all ranges, return the last color
        return self.color_scale[-1]['color']
    
    def map_grid(self, values: np.ndarray) -> List[List[str]]:
        """
        Map a 2D grid of values to colors
        
        Args:
            values: 2D numpy array of numerical values
            
        Returns:
            2D list of hex color strings
        """
        rows, cols = values.shape
        color_grid = []
        
        for i in range(rows):
            color_row = []
            for j in range(cols):
                color = self.map_value(values[i, j])
                color_row.append(color)
            color_grid.append(color_row)
        
        return color_grid
    
    def map_grid_with_alpha(self, values: np.ndarray, alpha: float = 0.7) -> List[List[str]]:
        """
        Map a 2D grid of values to RGBA colors with transparency
        
        Args:
            values: 2D numpy array of numerical values
            alpha: Transparency value (0-1)
            
        Returns:
            2D list of RGBA color strings
        """
        rows, cols = values.shape
        color_grid = []
        
        for i in range(rows):
            color_row = []
            for j in range(cols):
                hex_color = self.map_value(values[i, j])
                rgba_color = self._hex_to_rgba(hex_color, alpha)
                color_row.append(rgba_color)
            color_grid.append(color_row)
        
        return color_grid
    
    @staticmethod
    def _hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str:
        """
        Convert hex color to rgba string
        
        Args:
            hex_color: Hex color string (e.g., '#ffffff')
            alpha: Alpha value (0-1)
            
        Returns:
            RGBA string (e.g., 'rgba(255, 255, 255, 0.7)')
        """
        # Remove '#' if present
        hex_color = hex_color.lstrip('#')
        
        # Convert to RGB
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        
        return f'rgba({r}, {g}, {b}, {alpha})'
    
    def get_legend_items(self) -> List[Dict[str, Any]]:
        """
        Get legend items for display
        
        Returns:
            List of dicts with 'range', 'color', 'label' keys
        """
        legend_items = []
        
        for item in self.color_scale:
            legend_items.append({
                'min': item['min'],
                'max': item['max'],
                'color': item['color'],
                'label': f"{item['min']}-{item['max']}"
            })
        
        return legend_items


def get_rainfall_mapper() -> ColorMapper:
    """
    Get color mapper for rainfall data (matches frontend exactly)
    """
    rainfall_scale = [
        {'min': 0, 'max': 1, 'color': '#ffffff'},
        {'min': 1, 'max': 2, 'color': '#d3ffbe'},
        {'min': 2, 'max': 11, 'color': '#55ff00'},
        {'min': 11, 'max': 21, 'color': '#73dfff'},
        {'min': 21, 'max': 51, 'color': '#00a9e6'},
        {'min': 51, 'max': 71, 'color': '#ffaa00'},
        {'min': 71, 'max': 101, 'color': '#ff5a00'},
        {'min': 101, 'max': 999, 'color': '#ff0000'},
    ]
    return ColorMapper(rainfall_scale)


def get_temp_max_mapper() -> ColorMapper:
    """
    Get color mapper for maximum temperature (matches frontend exactly)
    """
    temp_max_scale = [
        {'min': 0, 'max': 15, 'color': '#70a800'},
        {'min': 15, 'max': 16, 'color': '#98e600'},
        {'min': 16, 'max': 21, 'color': '#e6e600'},
        {'min': 21, 'max': 26, 'color': '#ffaa00'},
        {'min': 26, 'max': 31, 'color': '#ff5a00'},
        {'min': 31, 'max': 36, 'color': '#c00000'},
        {'min': 36, 'max': 50, 'color': '#800000'},
    ]
    return ColorMapper(temp_max_scale)


def get_temp_min_mapper() -> ColorMapper:
    """
    Get color mapper for minimum temperature (matches frontend exactly)
    """
    temp_min_scale = [
        {'min': 0, 'max': 5, 'color': '#08306b'},
        {'min': 5, 'max': 6, 'color': '#0066ff'},
        {'min': 6, 'max': 11, 'color': '#00a884'},
        {'min': 11, 'max': 16, 'color': '#70a800'},
        {'min': 16, 'max': 21, 'color': '#98e600'},
        {'min': 21, 'max': 26, 'color': '#e6e600'},
        {'min': 26, 'max': 40, 'color': '#ffaa00'},
    ]
    return ColorMapper(temp_min_scale)


def get_rh_mapper() -> ColorMapper:
    """
    Get color mapper for relative humidity
    """
    rh_scale = [
        {'min': 0, 'max': 20, 'color': '#8B4513'},
        {'min': 20, 'max': 40, 'color': '#D2691E'},
        {'min': 40, 'max': 60, 'color': '#F0E68C'},
        {'min': 60, 'max': 80, 'color': '#90EE90'},
        {'min': 80, 'max': 100, 'color': '#00CED1'},
    ]
    return ColorMapper(rh_scale)


def get_cape_mapper() -> ColorMapper:
    """
    Get color mapper for CAPE
    """
    cape_scale = [
        {'min': 0, 'max': 500, 'color': '#E0E0E0'},
        {'min': 500, 'max': 1000, 'color': '#FFFF99'},
        {'min': 1000, 'max': 2000, 'color': '#FFCC66'},
        {'min': 2000, 'max': 3000, 'color': '#FF9933'},
        {'min': 3000, 'max': 5000, 'color': '#FF3333'},
        {'min': 5000, 'max': 10000, 'color': '#CC0000'},
    ]
    return ColorMapper(cape_scale)


def get_mapper_for_parameter(parameter_code: str) -> ColorMapper:
    """
    Get the appropriate color mapper for a parameter
    
    Args:
        parameter_code: Parameter code ('rainfall', 'temp-max', etc.)
        
    Returns:
        ColorMapper instance
    """
    mappers = {
        'rainfall': get_rainfall_mapper,
        'temp-max': get_temp_max_mapper,
        'temp-min': get_temp_min_mapper,
        'rh': get_rh_mapper,
        'cape': get_cape_mapper,
    }
    
    mapper_func = mappers.get(parameter_code)
    if mapper_func:
        return mapper_func()
    else:
        # Return a default grayscale mapper
        default_scale = [
            {'min': 0, 'max': 100, 'color': '#ffffff'},
            {'min': 100, 'max': 999999, 'color': '#000000'},
        ]
        return ColorMapper(default_scale)
