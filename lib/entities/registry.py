"""
Entity registry implementation and initialization

This module provides the registry initialization with all core entity types.
"""

from .base import EntityRegistry
from .core import register_core_entities


def create_default_registry() -> EntityRegistry:
    """
    Create and initialize a registry with all core entity types
    
    Returns:
        EntityRegistry: Initialized registry with core entities
    """
    registry = EntityRegistry()
    register_core_entities(registry)
    return registry