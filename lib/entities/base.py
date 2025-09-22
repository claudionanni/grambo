"""
Base classes for entity extraction system

This module provides the foundation classes for the entity-based parsing system:
- Entity: Base class for all extracted log entities
- Event: Represents a time-ordered log event
- Pattern: Represents a matching pattern for entity extraction
- EntityRegistry: Manages available entity types
"""

import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from enum import Enum
import re

# Import the new ID generation strategy
from .id_strategy import EntityIDGenerator


class EntityType(Enum):
    """Enumeration of supported entity types"""
    NODE = "NODE"
    STATE_TRANSFER = "STATE_TRANSFER"
    VIEW = "VIEW"
    COMMUNICATION = "COMMUNICATION"
    ERROR = "ERROR"
    WARNING = "WARNING"
    PERFORMANCE = "PERFORMANCE"
    TRANSACTION = "TRANSACTION"


class ConfidenceLevel(Enum):
    """Confidence levels for entity extraction"""
    LOW = 0.3
    MEDIUM = 0.6
    HIGH = 0.8
    VERY_HIGH = 0.95


@dataclass
class Entity(ABC):
    """
    Base class for all log entities extracted from Galera logs
    
    This class provides the common structure and interface for all
    entity types in the system.
    """
    
    # Core identification
    entity_id: str = field(default="")  # Will be generated in __post_init__
    entity_type: EntityType = field(init=False)
    
    # Temporal information
    timestamp: Optional[datetime] = None
    line_number: Optional[int] = None
    
    # Source information
    raw_line: str = ""
    log_source: str = ""
    
    # Extraction metadata
    confidence: float = 1.0
    pattern_name: str = ""
    extraction_method: str = "manual"
    
    # Validation status
    validated: bool = False
    validation_notes: str = ""

    def __post_init__(self):
        """Initialize entity ID and validate entity after initialization"""
        # Generate semantic ID if not already set
        if not self.entity_id:
            self.entity_id = self.generate_entity_id()
        self.validate()
    
    def generate_entity_id(self) -> str:
        """
        Generate semantic entity ID based on entity type and attributes
        
        Returns:
            str: Generated semantic entity ID
        """
        # Get entity type as string
        entity_type_str = self.entity_type.value if hasattr(self.entity_type, 'value') else str(self.entity_type)
        
        # Collect attributes for ID generation
        id_attrs = self.get_id_attributes()
        
        # Use EntityIDGenerator to create semantic ID
        return EntityIDGenerator.generate_entity_id(entity_type_str, **id_attrs)
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """
        Get attributes relevant for ID generation
        
        Subclasses should override this to provide entity-specific attributes
        
        Returns:
            Dict[str, Any]: Dictionary of attributes for ID generation
        """
        base_attrs = {
            'timestamp': self.timestamp,
        }
        
        # Add any attributes that exist on this entity
        for attr_name in ['node_name', 'node_address', 'cluster_name', 'view_id', 
                         'donor_node', 'joiner_node', 'error_type', 'seqno']:
            if hasattr(self, attr_name):
                base_attrs[attr_name] = getattr(self, attr_name)
        
        return base_attrs
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the entity's data integrity
        
        Returns:
            bool: True if entity is valid, False otherwise
            
        Raises:
            ValueError: If validation fails critically
        """
        pass
        
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert entity to dictionary representation
        
        Returns:
            Dict[str, Any]: Dictionary representation of the entity
        """
        # Handle timestamp formatting safely
        timestamp_str = None
        if self.timestamp:
            if hasattr(self.timestamp, 'isoformat'):
                timestamp_str = self.timestamp.isoformat()
            else:
                timestamp_str = str(self.timestamp)
        
        # Handle entity_type safely - could be enum or string
        if hasattr(self.entity_type, 'value'):
            entity_type_str = self.entity_type.value
        else:
            entity_type_str = str(self.entity_type)
        
        base_dict = {
            'entity_id': self.entity_id,
            'entity_type': entity_type_str,
            'timestamp': timestamp_str,
            'line_number': self.line_number,
            'raw_line': self.raw_line,
            'log_source': self.log_source,
            'confidence': self.confidence,
            'pattern_name': self.pattern_name,
            'extraction_method': self.extraction_method,
            'validated': self.validated,
            'validation_notes': self.validation_notes
        }
        return base_dict
        
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """
        Create entity from dictionary representation
        
        Args:
            data: Dictionary containing entity data
            
        Returns:
            Entity: Reconstructed entity instance
        """
        pass
        
    def update_confidence(self, new_confidence: float, reason: str = ""):
        """
        Update entity confidence with audit trail
        
        Args:
            new_confidence: New confidence value (0.0 to 1.0)
            reason: Reason for confidence update
        """
        if not 0.0 <= new_confidence <= 1.0:
            raise ValueError("Confidence must be between 0.0 and 1.0")
            
        old_confidence = self.confidence
        self.confidence = new_confidence
        
        if reason:
            if self.validation_notes:
                self.validation_notes += f"; Confidence updated from {old_confidence:.2f} to {new_confidence:.2f}: {reason}"
            else:
                self.validation_notes = f"Confidence updated from {old_confidence:.2f} to {new_confidence:.2f}: {reason}"
                
    def mark_validated(self, notes: str = ""):
        """
        Mark entity as validated by user
        
        Args:
            notes: Optional validation notes
        """
        self.validated = True
        if notes:
            if self.validation_notes:
                self.validation_notes += f"; Validated: {notes}"
            else:
                self.validation_notes = f"Validated: {notes}"


@dataclass
class Event(Entity):
    """
    Represents a time-ordered event in the log
    
    Events are entities that have a specific temporal order and
    represent state changes or significant occurrences.
    """
    
    # Event-specific fields
    event_name: str = ""
    event_category: str = ""
    
    # State information
    before_state: Optional[str] = None
    after_state: Optional[str] = None
    
    # Associated entities
    related_entities: List[str] = field(default_factory=list)
    
    # Duration if applicable
    duration_ms: Optional[float] = None
    
    def validate(self) -> bool:
        """Validate event data"""
        # Event name is optional for some entity types
        if not self.event_name and hasattr(self, 'entity_type'):
            # Auto-generate event name from entity type
            # Handle entity_type safely - could be enum or string
            if hasattr(self.entity_type, 'value'):
                entity_type_str = self.entity_type.value.lower()
            else:
                entity_type_str = str(self.entity_type).lower()
            self.event_name = f"{entity_type_str}_event"
            
        if self.duration_ms is not None and self.duration_ms < 0:
            raise ValueError("Duration cannot be negative")
            
        return True
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'event_name': self.event_name,
            'event_category': self.event_category,
            'before_state': self.before_state,
            'after_state': self.after_state,
            'related_entities': self.related_entities,
            'duration_ms': self.duration_ms
        })
        return base_dict
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary"""
        # Parse timestamp if present
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        # Create event instance
        event = cls(
            entity_id=data.get('entity_id', str(uuid.uuid4())),
            timestamp=timestamp,
            line_number=data.get('line_number'),
            raw_line=data.get('raw_line', ''),
            log_source=data.get('log_source', ''),
            confidence=data.get('confidence', 1.0),
            pattern_name=data.get('pattern_name', ''),
            extraction_method=data.get('extraction_method', 'manual'),
            validated=data.get('validated', False),
            validation_notes=data.get('validation_notes', ''),
            event_name=data.get('event_name', ''),
            event_category=data.get('event_category', ''),
            before_state=data.get('before_state'),
            after_state=data.get('after_state'),
            related_entities=data.get('related_entities', []),
            duration_ms=data.get('duration_ms')
        )
        
        # Set entity type
        if 'entity_type' in data:
            event.entity_type = EntityType(data['entity_type'])
            
        return event


@dataclass
class Pattern:
    """
    Represents a pattern for entity extraction
    
    Patterns define how to identify and extract specific entity types
    from log lines using regular expressions and metadata.
    """
    
    # Pattern identification
    name: str
    entity_type: EntityType
    version: str = "1.0"
    
    # Pattern definition
    regex: str = ""
    compiled_regex: Optional[re.Pattern] = field(default=None, init=False)
    
    # Extraction rules
    field_mappings: Dict[str, str] = field(default_factory=dict)
    required_fields: List[str] = field(default_factory=list)
    
    # Metadata
    description: str = ""
    examples: List[str] = field(default_factory=list)
    confidence: float = 0.8
    
    # Validation
    test_cases: List[Dict[str, Any]] = field(default_factory=list)
    
    def __post_init__(self):
        """Compile regex pattern after initialization"""
        if self.regex:
            try:
                self.compiled_regex = re.compile(self.regex, re.MULTILINE)
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{self.regex}': {e}")
                
    def match(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Attempt to match pattern against a log line
        
        Args:
            line: Log line to match against
            
        Returns:
            Dict[str, Any]: Extracted data if match found, None otherwise
        """
        if not self.compiled_regex:
            return None
            
        match = self.compiled_regex.search(line)
        if not match:
            return None
            
        # Extract named groups
        extracted_data = match.groupdict()
        
        # Apply field mappings
        mapped_data = {}
        for field, value in extracted_data.items():
            mapped_field = self.field_mappings.get(field, field)
            mapped_data[mapped_field] = value
            
        # Check required fields
        missing_fields = [field for field in self.required_fields if field not in mapped_data]
        if missing_fields:
            return None
            
        return mapped_data
        
    def test(self) -> List[Dict[str, Any]]:
        """
        Run pattern test cases
        
        Returns:
            List[Dict[str, Any]]: Test results with pass/fail status
        """
        results = []
        
        for test_case in self.test_cases:
            test_line = test_case.get('input', '')
            expected = test_case.get('expected', {})
            
            result = {
                'test_case': test_case,
                'input': test_line,
                'expected': expected,
                'actual': self.match(test_line),
                'passed': False,
                'notes': ''
            }
            
            if result['actual'] is None and not expected:
                result['passed'] = True
            elif result['actual'] is not None and expected:
                # Check if all expected fields are present and correct
                passed = True
                for key, value in expected.items():
                    if key not in result['actual'] or result['actual'][key] != value:
                        passed = False
                        result['notes'] = f"Field '{key}' mismatch: expected '{value}', got '{result['actual'].get(key)}'"
                        break
                result['passed'] = passed
            else:
                result['notes'] = "Match expectation mismatch"
                
            results.append(result)
            
        return results
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert pattern to dictionary representation"""
        # Handle entity_type safely - could be enum or string
        if hasattr(self.entity_type, 'value'):
            entity_type_str = self.entity_type.value
        else:
            entity_type_str = str(self.entity_type)
            
        return {
            'name': self.name,
            'entity_type': entity_type_str,
            'version': self.version,
            'regex': self.regex,
            'field_mappings': self.field_mappings,
            'required_fields': self.required_fields,
            'description': self.description,
            'examples': self.examples,
            'confidence': self.confidence,
            'test_cases': self.test_cases
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Pattern':
        """Create pattern from dictionary representation"""
        return cls(
            name=data['name'],
            entity_type=EntityType(data['entity_type']),
            version=data.get('version', '1.0'),
            regex=data.get('regex', ''),
            field_mappings=data.get('field_mappings', {}),
            required_fields=data.get('required_fields', []),
            description=data.get('description', ''),
            examples=data.get('examples', []),
            confidence=data.get('confidence', 0.8),
            test_cases=data.get('test_cases', [])
        )


class EntityRegistry:
    """
    Registry for managing available entity types and their patterns
    
    The registry maintains the catalog of entity types and provides
    factory methods for creating instances.
    """
    
    def __init__(self):
        self._entity_classes: Dict[EntityType, type] = {}
        self._patterns: Dict[EntityType, List[Pattern]] = {}
        
    def register_entity_class(self, entity_type: EntityType, entity_class: type):
        """
        Register an entity class for a specific type
        
        Args:
            entity_type: The entity type enum
            entity_class: The class to use for this entity type
        """
        if not issubclass(entity_class, Entity):
            raise ValueError("Entity class must inherit from Entity")
            
        self._entity_classes[entity_type] = entity_class
        
    def register_pattern(self, pattern: Pattern):
        """
        Register a pattern for entity extraction
        
        Args:
            pattern: Pattern to register
        """
        if pattern.entity_type not in self._patterns:
            self._patterns[pattern.entity_type] = []
            
        self._patterns[pattern.entity_type].append(pattern)
        
    def get_entity_class(self, entity_type: EntityType) -> Optional[type]:
        """
        Get the registered class for an entity type
        
        Args:
            entity_type: The entity type to look up
            
        Returns:
            type: The registered entity class, or None if not found
        """
        return self._entity_classes.get(entity_type)
        
    def get_patterns(self, entity_type: EntityType) -> List[Pattern]:
        """
        Get all patterns for a specific entity type
        
        Args:
            entity_type: The entity type to get patterns for
            
        Returns:
            List[Pattern]: List of patterns for the entity type
        """
        return self._patterns.get(entity_type, [])
        
    def get_all_patterns(self) -> Dict[EntityType, List[Pattern]]:
        """
        Get all registered patterns
        
        Returns:
            Dict[EntityType, List[Pattern]]: All patterns organized by type
        """
        return self._patterns.copy()
        
    def create_entity(self, entity_type: EntityType, **kwargs) -> Optional[Entity]:
        """
        Create an entity instance of the specified type
        
        Args:
            entity_type: Type of entity to create
            **kwargs: Arguments to pass to entity constructor
            
        Returns:
            Entity: New entity instance, or None if type not registered
        """
        entity_class = self.get_entity_class(entity_type)
        if not entity_class:
            return None
            
        try:
            entity = entity_class(**kwargs)
            entity.entity_type = entity_type
            return entity
        except Exception as e:
            # Better error reporting for debugging
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Failed to create entity {entity_type}: {e}")
            return None
            
    def list_entity_types(self) -> List[EntityType]:
        """
        Get list of all registered entity types
        
        Returns:
            List[EntityType]: List of registered entity types
        """
        return list(self._entity_classes.keys())
        
    def validate_patterns(self) -> Dict[EntityType, List[Dict[str, Any]]]:
        """
        Validate all registered patterns by running their test cases
        
        Returns:
            Dict[EntityType, List[Dict[str, Any]]]: Validation results by entity type
        """
        results = {}
        
        for entity_type, patterns in self._patterns.items():
            results[entity_type] = []
            
            for pattern in patterns:
                pattern_results = pattern.test()
                results[entity_type].append({
                    'pattern_name': pattern.name,
                    'test_results': pattern_results,
                    'passed': all(r['passed'] for r in pattern_results),
                    'total_tests': len(pattern_results),
                    'passed_tests': sum(1 for r in pattern_results if r['passed'])
                })
                
        return results