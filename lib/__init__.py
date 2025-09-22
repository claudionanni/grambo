"""
GRAP - Galera Regex Analysis Parser (Next Generation)

This package provides entity-based log parsing for MariaDB/Galera cluster analysis.
"""

__version__ = "2.0.0-alpha1"
__author__ = "Claudio Nanni"
__description__ = "Galera Regex Analysis Parser - Entity-based log analysis"

from .entities import *
from .patterns import *
from .output import *
from .parser import LogParser

__all__ = [
    # Core classes
    'LogParser',
    
    # From entities
    'Entity', 'Event', 'Pattern', 'EntityRegistry', 'EntityType', 'ConfidenceLevel',
    'NodeEntity', 'StateTransferEntity', 'ViewEntity',
    'NodeState', 'StateTransferType', 'StateTransferMethod',
    'create_default_registry',
    
    # From patterns
    'PatternMatcher',
    
    # From output
    'OutputFormatter'
]