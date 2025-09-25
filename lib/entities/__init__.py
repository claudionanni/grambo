"""
Entity extraction system for Galera cluster log analysis

This package provides the comprehensive entity extraction and analysis system:
- Base classes for entities, events, and patterns
- Core entity implementations (Node, SST, View, etc.)
- Hierarchical cluster entity with split-brain detection
- Enhanced multi-log parser with pattern-based extraction
- Entity registry for managing types and patterns
- Temporal analysis capabilities
- Session management for tracking entity relationships
"""

# Import base classes
from .base import Entity, Event, Pattern, EntityRegistry, EntityType, ConfidenceLevel

# Import core entity implementations  
from .core import (
    NodeEntity, StateTransferEntity, ViewEntity, CommunicationEntity,
    WarningEntity, ErrorEntity, PerformanceEntity, TransactionEntity,
    NodeState, StateTransferType, StateTransferMethod,
    register_core_entities
)

# Import hierarchical cluster entity
from .cluster import ClusterEntity, ViewCollection, MemberCollection

# Import enhanced parser
from .enhanced_parser import (
    MultiLogParser, create_enhanced_parser, 
    parse_galera_logs, analyze_cluster_from_directory
)

# Import utility classes
try:
    from .temporal import TemporalProperty, TemporalPropertySet
except ImportError:
    TemporalProperty = None
    TemporalPropertySet = None

try:
    from .relationships import EntityRelationship, RelationshipType, RelationshipManager
except ImportError:
    EntityRelationship = None
    RelationshipType = None
    RelationshipManager = None

try:
    from .registry import create_default_registry
    EntityRegistryManager = None  # Not implemented yet
except ImportError:
    create_default_registry = None
    EntityRegistryManager = None

try:
    from .session_manager import SessionManager
except ImportError:
    SessionManager = None

from .id_strategy import EntityIDGenerator

__all__ = [
    # Base classes
    'Entity', 'Event', 'Pattern', 'EntityRegistry', 'EntityType', 'ConfidenceLevel',
    
    # Core entities
    'NodeEntity', 'StateTransferEntity', 'ViewEntity', 'CommunicationEntity',
    'WarningEntity', 'ErrorEntity', 'PerformanceEntity', 'TransactionEntity',
    
    # Hierarchical cluster entity
    'ClusterEntity', 'ViewCollection', 'MemberCollection',
    
    # Enhanced parsing
    'MultiLogParser', 'create_enhanced_parser', 
    'parse_galera_logs', 'analyze_cluster_from_directory',
    
    # Enums
    'NodeState', 'StateTransferType', 'StateTransferMethod',
    
    # Utility classes (may be None if not available)
    'TemporalProperty', 'TemporalPropertySet',
    'EntityRelationship', 'RelationshipType', 'RelationshipManager',
    'EntityRegistryManager', 'SessionManager', 'EntityIDGenerator',
    
    # Registration function
    'register_core_entities',
    
    # Factory functions (if available)
    'create_default_registry'
]