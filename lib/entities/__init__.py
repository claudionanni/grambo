"""
Entity classes for Galera log analysis

This package provides entity classes for parsing and analyzing Galera cluster logs.
"""

from .base import Entity, Event, Pattern, EntityRegistry, EntityType, ConfidenceLevel
from .core import NodeEntity, StateTransferEntity, ViewEntity, NodeState, StateTransferType, StateTransferMethod
from .registry import create_default_registry

__all__ = [
    # Base classes
    'Entity', 'Event', 'Pattern', 'EntityRegistry', 'EntityType', 'ConfidenceLevel',
    
    # Core entities
    'NodeEntity', 'StateTransferEntity', 'ViewEntity',
    
    # Enums
    'NodeState', 'StateTransferType', 'StateTransferMethod',
    
    # Factory functions
    'create_default_registry'
]