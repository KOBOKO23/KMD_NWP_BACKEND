"""
Color Mapper Utility - Official KMD Standard
Maps WRF output values to official KMD color legend (contours)
File: wrf_data/utils/color_mapper.py

Based on operational standard:
- Rainfall: 0mm = WHITE (no rain), then colored bins
- Temperatures: All regions have color (no zero temps in Kenya)
- Exact RGB values from official standard
"""

import numpy as np
from typing import List, Dict, Any


TRANSPARENT = 'rgba(0,0,0,0)'


class ColorMapper:
    """
    Maps numerical weather data to colors based on official KMD legend bins
    """

    def __init__(self, color_scale: List[Dict[str, Any]]):
        self.color_scale = sorted(color_scale, key=lambda x: x['min'])

    def map_value(self, value: float) -> str:
        """
        Map a single value to hex color
        """
        if value is None or np.isnan(value):
            return TRANSPARENT

        # Find the appropriate bin
        for item in self.color_scale:
            if item['min'] <= value < item['max']:
                return item['color']

        # If above all bins, use last color
        if value >= self.color_scale[-1]['max']:
            return self.color_scale[-1]['color']
        
        # If below all bins, use first color
        return self.color_scale[0]['color']

    def map_grid(self, values: np.ndarray) -> List[List[str]]:
        """
        Map entire grid to colors
        """
        rows, cols = values.shape
        return [
            [self.map_value(values[i, j]) for j in range(cols)]
            for i in range(rows)
        ]

    def map_grid_with_alpha(self, values: np.ndarray, alpha: float = 0.8) -> List[List[str]]:
        """
        Map grid with transparency (for layered maps)
        """
        rows, cols = values.shape
        grid = []

        for i in range(rows):
            row = []
            for j in range(cols):
                v = values[i, j]

                if v is None or np.isnan(v):
                    row.append(TRANSPARENT)
                else:
                    hex_color = self.map_value(v)
                    if hex_color == TRANSPARENT:
                        row.append(TRANSPARENT)
                    else:
                        row.append(self._hex_to_rgba(hex_color, alpha))
            grid.append(row)

        return grid

    @staticmethod
    def _hex_to_rgba(hex_color: str, alpha: float) -> str:
        """Convert hex color to rgba with alpha"""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f'rgba({r},{g},{b},{alpha})'

    def get_legend_items(self) -> List[Dict[str, Any]]:
        """
        Legend items for frontend display
        """
        return [
            {
                'min': item['min'],
                'max': item['max'],
                'color': item['color'],
                'label': f"{item['min']}–{item['max']}" if item['max'] < 999 else f">{item['min']}"
            }
            for item in self.color_scale
        ]


# ============================================================
# RAINFALL – OFFICIAL KMD STANDARD (from your image)
# ============================================================

def get_rainfall_mapper() -> ColorMapper:
    """
    Official KMD rainfall color scale
    
    Key: 0 mm = WHITE (no rain), not transparent or gray
    Based on "7 contours" table from operational standard
    """
    rainfall_scale = [
        # RGB values from your image
        {'min': 0.0,   'max': 1.0,   'color': '#ffffff'},  # < 1mm - WHITE (R:255, G:255, B:255)
        {'min': 1.0,   'max': 10.0,  'color': '#d3ff55'},  # 2-10 mm - Light green (R:211, G:255, B:85)
        {'min': 10.0,  'max': 20.0,  'color': '#73ff55'},  # 11-20 mm - Green (R:115, G:255, B:85)
        {'min': 20.0,  'max': 50.0,  'color': '#55dfff'},  # 21-50 mm - Light blue (R:85, G:223, B:255)
        {'min': 50.0,  'max': 70.0,  'color': '#55a9ff'},  # 51-70 mm - Blue (R:85, G:169, B:255)
        {'min': 70.0,  'max': 100.0, 'color': '#ffaa00'},  # 71-100 mm - Orange (R:255, G:170, B:0)
        {'min': 100.0, 'max': 120.0, 'color': '#ff5500'},  # 101-120 mm - Dark orange (R:255, G:85, B:0)
        {'min': 120.0, 'max': 9999,  'color': '#ff0000'},  # >121 mm - Red (R:255, G:0, B:0)
    ]
    return ColorMapper(rainfall_scale)


# ============================================================
# TEMPERATURE MAX – OFFICIAL KMD STANDARD
# ============================================================

def get_temp_max_mapper() -> ColorMapper:
    """
    Official KMD maximum temperature color scale
    Based on "Max" table from operational standard
    
    Note: All areas have color - no zero temperatures in Kenya
    """
    temp_max_scale = [
        # RGB values from your image - "Max" table
        {'min': 0.0,  'max': 15.0, 'color': '#70a800'},  # 0-15°C - Green (R:112, G:168, B:0)
        {'min': 15.0, 'max': 20.0, 'color': '#98e600'},  # 16-20°C - Light green (R:152, G:230, B:0)
        {'min': 20.0, 'max': 25.0, 'color': '#e6e600'},  # 21-25°C - Yellow (R:230, G:230, B:0)
        {'min': 25.0, 'max': 30.0, 'color': '#ffaa00'},  # 26-30°C - Orange (R:255, G:170, B:0)
        {'min': 30.0, 'max': 35.0, 'color': '#ff5a00'},  # 31-35°C - Dark orange (R:255, G:90, B:0)
        {'min': 35.0, 'max': 9999, 'color': '#c00000'},  # >36°C - Red (R:192, G:0, B:0)
    ]
    return ColorMapper(temp_max_scale)


# ============================================================
# TEMPERATURE MIN – OFFICIAL KMD STANDARD
# ============================================================

def get_temp_min_mapper() -> ColorMapper:
    """
    Official KMD minimum temperature color scale
    Based on "Min" table from operational standard
    
    Note: All areas have color - no zero temperatures in Kenya
    """
    temp_min_scale = [
        # RGB values from your image - "Min" table
        {'min': 0.0,  'max': 5.0,  'color': '#00006b'},  # < 5°C - Dark blue (R:0, G:0, B:107)
        {'min': 5.0,  'max': 10.0, 'color': '#0030ff'},  # 6-10°C - Blue (R:0, G:48, B:255)
        {'min': 10.0, 'max': 15.0, 'color': '#00a8a8'},  # 11-15°C - Cyan (R:0, G:168, B:168)
        {'min': 15.0, 'max': 20.0, 'color': '#70a800'},  # 16-20°C - Green (R:112, G:168, B:0)
        {'min': 20.0, 'max': 25.0, 'color': '#98e600'},  # 21-25°C - Light green (R:152, G:230, B:0)
        {'min': 25.0, 'max': 9999, 'color': '#e6e600'},  # >26°C - Yellow (R:230, G:230, B:0)
    ]
    return ColorMapper(temp_min_scale)


# ============================================================
# RELATIVE HUMIDITY (Generic - not in your image)
# ============================================================

def get_rh_mapper() -> ColorMapper:
    """
    Relative humidity color scale
    Generic scale (not shown in operational standard image)
    """
    rh_scale = [
        {'min': 0,   'max': 20,  'color': '#8B4513'},  # 0-20% - Brown (dry)
        {'min': 20,  'max': 40,  'color': '#D2691E'},  # 20-40% - Light brown
        {'min': 40,  'max': 60,  'color': '#F0E68C'},  # 40-60% - Tan
        {'min': 60,  'max': 80,  'color': '#90EE90'},  # 60-80% - Light green
        {'min': 80,  'max': 100, 'color': '#00CED1'},  # 80-100% - Cyan (wet)
    ]
    return ColorMapper(rh_scale)


# ============================================================
# CAPE (Generic - not in your image)
# ============================================================

def get_cape_mapper() -> ColorMapper:
    """
    CAPE (Convective Available Potential Energy) color scale
    Generic scale for instability indication
    """
    cape_scale = [
        {'min': 0,    'max': 500,   'color': '#f0f0f0'},  # 0-500 - Light gray (stable)
        {'min': 500,  'max': 1000,  'color': '#ffff99'},  # 500-1000 - Light yellow
        {'min': 1000, 'max': 2000,  'color': '#ffcc66'},  # 1000-2000 - Yellow-orange
        {'min': 2000, 'max': 3000,  'color': '#ff9933'},  # 2000-3000 - Orange
        {'min': 3000, 'max': 5000,  'color': '#ff3333'},  # 3000-5000 - Red
        {'min': 5000, 'max': 99999, 'color': '#cc0000'},  # >5000 - Dark red (very unstable)
    ]
    return ColorMapper(cape_scale)


# ============================================================
# PARAMETER ROUTER
# ============================================================

def get_mapper_for_parameter(parameter_code: str) -> ColorMapper:
    """
    Get the appropriate color mapper for a parameter
    """
    mappers = {
        'rainfall': get_rainfall_mapper,
        'temp-max': get_temp_max_mapper,
        'temp-min': get_temp_min_mapper,
        'rh': get_rh_mapper,
        'cape': get_cape_mapper,
    }

    mapper_func = mappers.get(parameter_code, get_rainfall_mapper)
    return mapper_func()