"""
Types for streaming data to viewers.

This module provides type-safe enums for data types used in streaming
backends and visualizers. These are generic types used by any application
that streams image data to viewers.
"""

from enum import Enum


class StreamingDataType(Enum):
    """Types of data that can be streamed to viewers."""
    IMAGE = 'image'
    SHAPES = 'shapes'  # For Napari shapes layer
    POINTS = 'points'  # For Napari points layer (e.g., skeleton tracings)
    ROIS = 'rois'      # For Fiji


class NapariShapeType(Enum):
    """Napari shape types for ROI visualization."""
    POLYGON = 'polygon'
    ELLIPSE = 'ellipse'
    POINT = 'point'
    LINE = 'line'
    PATH = 'path'
    RECTANGLE = 'rectangle'
