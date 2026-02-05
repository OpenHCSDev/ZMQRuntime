"""
Explicit protocols for viewer configuration.
Defines clear contracts between config providers and consumers.
No hasattr, no getattr, no fallbacks, no protocols.
Explicit isinstance checks and direct access.
"""

from typing import Any, Dict, List, Union
from enum import Enum


class ComponentMode(Enum):
    """How to handle a component dimension in visualization."""
    STACK = "stack"  # Stack images along this dimension
    SLICE = "slice"  # Create separate layers for each value


class DictDisplayConfig:
    """
    Dict-based display config.
    
    Direct dict access only.
    No hasattr, no getattr, no protocols, no fallbacks.
    Fails loud if config doesn't have expected keys.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self._config = config
    
    @property
    def component_modes(self) -> Dict[str, str]:
        """Direct dict access."""
        modes = self._config.get("component_modes")
        if modes is not None:
            return modes
        return {}
    
    @property
    def component_order(self) -> List[str]:
        """Direct dict access."""
        return self._config.get("component_order", [])


class ObjectDisplayConfig:
    """
    Object-based display config.
    
    Direct attribute access only.
    No hasattr, no getattr, no protocols, no fallbacks.
    Fails loud if config doesn't have expected attributes.
    """
    
    def __init__(self, config: object):
        if not hasattr(config, 'COMPONENT_ORDER'):
            raise AttributeError(f"ObjectDisplayConfig requires COMPONENT_ORDER attribute")
        self._config = config
    
    @property
    def component_modes(self) -> Dict[str, str]:
        """Direct attribute access."""
        component_modes = {}
        for component in self._config.COMPONENT_ORDER:
            mode_attr = f"{component}_mode"
            mode_value = self._config.__dict__[mode_attr]
            if isinstance(mode_value, str):
                component_modes[component] = mode_value.lower()
            else:
                component_modes[component] = mode_value.value
        return component_modes
    
    @property
    def component_order(self) -> List[str]:
        """Direct attribute access."""
        return list(self._config.COMPONENT_ORDER)
