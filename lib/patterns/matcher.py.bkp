"""
Pattern matching system for entity extraction

This module provides the core pattern matching functionality for extracting
entities from Galera log lines using configurable regex patterns with
dialect awareness.
"""

import re
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime
import logging

from ..entities import EntityType, Pattern, Entity, create_default_registry
from .dialect import DialectDetector, DialectPatternManager, DialectType, DialectInfo


class PatternMatcher:
    """
    Core pattern matching engine for entity extraction with dialect awareness
    
    This class manages pattern loading, matching, and confidence scoring
    for extracting entities from log lines, with automatic dialect detection
    and appropriate pattern selection.
    """
    
    def __init__(self, pattern_dir: Path, version: Optional[str] = None, 
                 confidence_threshold: float = 0.8, dialect: Optional[str] = None):
        """
        Initialize pattern matcher with dialect awareness
        
        Args:
            pattern_dir: Directory containing pattern files
            version: Specific MariaDB version patterns to load (legacy)
            confidence_threshold: Minimum confidence for entity extraction
            dialect: Force specific dialect (optional, auto-detected if None)
        """
        self.pattern_dir = Path(pattern_dir)
        self.version = version
        self.confidence_threshold = confidence_threshold
        self.logger = logging.getLogger(__name__)
        
        # Dialect detection and management
        self.dialect_detector = DialectDetector()
        self.dialect_manager = DialectPatternManager(pattern_dir)
        self.forced_dialect = DialectType(dialect) if dialect else None
        self.detected_dialect: Optional[DialectInfo] = None
        self.current_dialect: DialectType = DialectType.DEFAULT
        
        # Pattern storage
        self._patterns: Dict[EntityType, List[Pattern]] = {}
        self._compiled_patterns: Dict[EntityType, List[Tuple[Pattern, re.Pattern]]] = {}
        
        # Entity registry for creation
        self.entity_registry = create_default_registry()
        
        # Load patterns with dialect awareness
        self._load_patterns()
        
    def detect_dialect_from_file(self, file_path: Path) -> DialectInfo:
        """Detect dialect from log file and reload patterns if needed"""
        if self.forced_dialect:
            return DialectInfo(
                dialect_type=self.forced_dialect,
                version="forced",
                confidence=1.0,
                detection_method="forced_by_user",
                features=[]
            )
            
        dialect_info = self.dialect_detector.detect_from_file(file_path)
        self.detected_dialect = dialect_info
        
        # Reload patterns if dialect changed
        if self.current_dialect != dialect_info.dialect_type:
            old_dialect = self.current_dialect
            self.current_dialect = dialect_info.dialect_type
            
            self.logger.info(f"Detected dialect: {self.current_dialect.value} "
                            f"(confidence: {dialect_info.confidence:.2f}, "
                            f"method: {dialect_info.detection_method})")
            
            # Reload patterns for the detected dialect
            self.logger.info(f"Switching from {old_dialect.value} to {self.current_dialect.value}, reloading patterns")
            self._load_patterns()
        
        return dialect_info
    
    def set_dialect(self, dialect: Union[str, DialectType]) -> None:
        """Manually set dialect and reload patterns"""
        if isinstance(dialect, str):
            dialect = DialectType(dialect)
            
        old_dialect = self.current_dialect
        self.current_dialect = dialect
        
        if old_dialect != dialect:
            self.logger.info(f"Switching dialect from {old_dialect.value} to {dialect.value}")
            self._load_patterns()
    
    def _load_patterns(self):
        """Load patterns from YAML files with dialect awareness"""
        self.logger.info(f"Loading patterns for dialect: {self.current_dialect.value}")
        
        # Get appropriate pattern files for current dialect
        pattern_files = self.dialect_manager.get_pattern_files(self.current_dialect)
        
        if not pattern_files:
            self.logger.warning(f"No pattern files found for dialect {self.current_dialect.value}")
            return
        
        # Load patterns from files
        total_patterns = 0
        for pattern_file in pattern_files:
            try:
                patterns_loaded = self._load_pattern_file_dialect_aware(pattern_file)
                total_patterns += patterns_loaded
                self.logger.debug(f"Loaded {patterns_loaded} patterns from {pattern_file.name}")
            except Exception as e:
                self.logger.error(f"Failed to load patterns from {pattern_file}: {e}")
        
        # Compile patterns for performance
        self._compile_patterns()
        
        entity_types = len(self._patterns)
        self.logger.info(f"Loaded {total_patterns} patterns for {entity_types} entity types")
    
    def _load_pattern_file_dialect_aware(self, pattern_file: Path) -> int:
        """Load patterns from a single YAML file with dialect adaptations"""
        try:
            with open(pattern_file, 'r', encoding='utf-8') as f:
                patterns_data = yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Failed to parse {pattern_file}: {e}")
            return 0
        
        if not patterns_data or 'patterns' not in patterns_data:
            self.logger.warning(f"No patterns found in {pattern_file}")
            return 0
        
        # Adapt patterns for current dialect
        patterns_data['patterns'] = self.dialect_manager.adapt_patterns_for_dialect(
            patterns_data['patterns'], self.current_dialect
        )
        
        patterns_loaded = 0
        for entity_type_name, pattern_list in patterns_data['patterns'].items():
            try:
                entity_type = EntityType(entity_type_name.upper())
            except ValueError as e:
                self.logger.warning(f"Unknown entity type '{entity_type_name}' in {pattern_file}: {e}")
                continue
                
            if entity_type not in self._patterns:
                self._patterns[entity_type] = []
            
            for pattern_data in pattern_list:
                try:
                    pattern = self._create_pattern(pattern_data, entity_type)
                    if pattern:
                        self._patterns[entity_type].append(pattern)
                        patterns_loaded += 1
                except Exception as e:
                    self.logger.error(f"Failed to create pattern from {pattern_data}: {e}")
        
        return patterns_loaded
                        
    def _load_pattern_file(self, pattern_file: Path):
        """Load patterns from a single YAML file"""
        try:
            with open(pattern_file, 'r') as f:
                data = yaml.safe_load(f)
                
            # Validate file structure
            if not isinstance(data, dict):
                raise ValueError("Pattern file must contain a dictionary")
                
            # Check version compatibility if specified
            file_version = data.get('version', '')
            if self.version and file_version:
                if not self._is_compatible_version(file_version, self.version):
                    self.logger.debug(f"Skipping {pattern_file} - version mismatch: "
                                    f"file={file_version}, requested={self.version}")
                    return
                    
            # Load patterns
            patterns_data = data.get('patterns', {})
            for entity_type_name, pattern_list in patterns_data.items():
                try:
                    entity_type = EntityType(entity_type_name.upper())
                    
                    if entity_type not in self._patterns:
                        self._patterns[entity_type] = []
                        
                    for pattern_data in pattern_list:
                        pattern = self._create_pattern(pattern_data, entity_type)
                        if pattern:
                            self._patterns[entity_type].append(pattern)
                            
                except ValueError as e:
                    self.logger.warning(f"Unknown entity type '{entity_type_name}' in {pattern_file}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error loading pattern file {pattern_file}: {e}")
            raise
            
    def _create_pattern(self, pattern_data: Dict[str, Any], entity_type: EntityType) -> Optional[Pattern]:
        """Create a Pattern object from YAML data"""
        try:
            pattern = Pattern(
                name=pattern_data['name'],
                entity_type=entity_type,
                version=pattern_data.get('version', '1.0'),
                regex=pattern_data.get('regex', ''),
                field_mappings=pattern_data.get('field_mappings', {}),
                required_fields=pattern_data.get('required_fields', []),
                description=pattern_data.get('description', ''),
                examples=pattern_data.get('examples', []),
                confidence=pattern_data.get('confidence', 0.8),
                test_cases=pattern_data.get('test_cases', [])
            )
            
            # Validate pattern
            if not pattern.regex:
                self.logger.warning(f"Pattern '{pattern.name}' has no regex")
                return None
                
            return pattern
            
        except Exception as e:
            self.logger.error(f"Error creating pattern from data: {e}")
            return None
            
    def _compile_patterns(self):
        """Compile all loaded regex patterns for efficient matching"""
        self._compiled_patterns = {}
        
        for entity_type, patterns in self._patterns.items():
            self._compiled_patterns[entity_type] = []
            
            for pattern in patterns:
                if pattern.compiled_regex:
                    self._compiled_patterns[entity_type].append((pattern, pattern.compiled_regex))
                    
    def _is_compatible_version(self, file_version: str, requested_version: str) -> bool:
        """
        Check if pattern file version is compatible with requested version
        
        Args:
            file_version: Version specified in pattern file
            requested_version: Requested MariaDB version
            
        Returns:
            bool: True if compatible
        """
        # Simple version matching for now
        # TODO: Implement semantic version compatibility
        return file_version.startswith(requested_version[:4])  # Match major.minor
        
    def match_line(self, line: str, entity_types: Optional[List[EntityType]] = None) -> List[Entity]:
        """
        Match a log line against patterns and extract entities
        
        Args:
            line: Log line to process
            entity_types: Optional list of entity types to match against
            
        Returns:
            List[Entity]: List of extracted entities (deduplicated)
        """
        entities = []
        
        # Default to all entity types if none specified
        if entity_types is None:
            entity_types = list(self._compiled_patterns.keys())
            
        for entity_type in entity_types:
            if entity_type not in self._compiled_patterns:
                continue
                
            for pattern, compiled_regex in self._compiled_patterns[entity_type]:
                try:
                    # Skip patterns below confidence threshold
                    if pattern.confidence < self.confidence_threshold:
                        continue
                        
                    # Try to match
                    match = compiled_regex.search(line)
                    if match:
                        entity = self._create_entity_from_match(
                            pattern, match, line, entity_type
                        )
                        if entity:
                            entities.append(entity)
                            
                except Exception as e:
                    self.logger.error(f"Error matching pattern '{pattern.name}': {e}")
        
        # Deduplicate entities based on semantic similarity
        deduplicated_entities = self._deduplicate_entities(entities)
        return deduplicated_entities
    
    def _deduplicate_entities(self, entities: List[Entity]) -> List[Entity]:
        """
        Remove duplicate entities that represent the same information
        
        Args:
            entities: List of entities to deduplicate
            
        Returns:
            List[Entity]: Deduplicated entities (keeping highest confidence)
        """
        if not entities:
            return entities
            
        # Group entities by type and semantic key
        entity_groups = {}
        
        for entity in entities:
            # Create a semantic key based on entity type and key fields
            semantic_key = self._get_semantic_key(entity)
            
            if semantic_key not in entity_groups:
                entity_groups[semantic_key] = []
            entity_groups[semantic_key].append(entity)
        
        # For each group, keep the entity with highest confidence
        deduplicated = []
        for group in entity_groups.values():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                # Keep the entity with highest confidence
                best_entity = max(group, key=lambda e: e.confidence)
                deduplicated.append(best_entity)
                
                # Log deduplication info
                pattern_names = [e.pattern_name for e in group]
                self.logger.debug(f"Deduplicated {len(group)} entities: {pattern_names} -> kept {best_entity.pattern_name}")
        
        return deduplicated
    
    def _get_semantic_key(self, entity: Entity) -> str:
        """
        Generate a semantic key for entity deduplication
        
        Args:
            entity: Entity to generate key for
            
        Returns:
            str: Semantic key representing the entity's core identity
        """
        # Handle entity_type safely - could be enum or string
        if hasattr(entity.entity_type, 'value'):
            entity_type_str = entity.entity_type.value
        else:
            entity_type_str = str(entity.entity_type)
            
        key_parts = [entity_type_str, entity.raw_line]
        
        # Add type-specific identifying fields
        if hasattr(entity, 'view_id') and entity.view_id:
            key_parts.append(f"view_id:{entity.view_id}")
        if hasattr(entity, 'view_seq') and entity.view_seq:
            key_parts.append(f"view_seq:{entity.view_seq}")
        if hasattr(entity, 'cluster_state') and entity.cluster_state:
            key_parts.append(f"cluster_state:{entity.cluster_state}")
        if hasattr(entity, 'node_id') and entity.node_id:
            key_parts.append(f"node_id:{entity.node_id}")
        if hasattr(entity, 'transfer_type') and entity.transfer_type:
            key_parts.append(f"transfer_type:{entity.transfer_type}")
        if hasattr(entity, 'session_id') and entity.session_id:
            key_parts.append(f"session_id:{entity.session_id}")
            
        return "|".join(key_parts)
        
    def _create_entity_from_match(self, pattern: Pattern, match: re.Match, 
                                 line: str, entity_type: EntityType) -> Optional[Entity]:
        """
        Create an entity from a successful pattern match
        
        Args:
            pattern: Pattern that matched
            match: Regex match object
            line: Original log line
            entity_type: Type of entity to create
            
        Returns:
            Entity: Created entity or None if creation failed
        """
        try:
            # Extract data from match
            extracted_data = match.groupdict()
            
            # Apply field mappings
            mapped_data = {}
            for field, value in extracted_data.items():
                mapped_field = pattern.field_mappings.get(field, field)
                mapped_data[mapped_field] = value
                
            # Parse timestamp if present
            timestamp = None
            if 'timestamp' in mapped_data and mapped_data['timestamp']:
                timestamp = self._parse_timestamp(mapped_data['timestamp'])
                
            # Create base entity data
            entity_data = {
                'raw_line': line,
                'timestamp': timestamp,
                'confidence': pattern.confidence,
                'pattern_name': pattern.name,
                'extraction_method': 'pattern_match'
            }
            
            # Add pattern-specific data
            entity_data.update(mapped_data)
            
            # Set default values for entity types that need them
            if entity_type == EntityType.STATE_TRANSFER:
                if 'transfer_type' not in entity_data:
                    from ..entities.core import StateTransferType
                    entity_data['transfer_type'] = StateTransferType.SST  # Default to SST enum
                elif isinstance(entity_data['transfer_type'], str):
                    # Convert string to enum
                    from ..entities.core import StateTransferType
                    try:
                        entity_data['transfer_type'] = StateTransferType(entity_data['transfer_type'])
                    except ValueError:
                        entity_data['transfer_type'] = StateTransferType.SST  # Default fallback
            
            # Create entity using registry
            entity = self.entity_registry.create_entity(entity_type, **entity_data)
            
            return entity
            
        except Exception as e:
            self.logger.error(f"Error creating entity from match: {e}")
            return None
            
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp from various MariaDB log formats
        
        Args:
            timestamp_str: Timestamp string from log
            
        Returns:
            datetime: Parsed timestamp or None if parsing failed
        """
        # Safety check for None or empty timestamp
        if not timestamp_str:
            return None
            
        # Common MariaDB timestamp formats
        formats = [
            "%Y-%m-%d %H:%M:%S",           # Standard format
            "%Y-%m-%dT%H:%M:%S",           # ISO format
            "%Y-%m-%d %H:%M:%S.%f",        # With microseconds
            "%Y-%m-%dT%H:%M:%S.%f",        # ISO with microseconds
            "%m%d %H:%M:%S",               # Short format
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str.strip(), fmt)
            except ValueError:
                continue
                
        # If no format matches, log warning but don't fail
        self.logger.debug(f"Could not parse timestamp: {timestamp_str}")
        return None
        
    def get_patterns(self, entity_type: EntityType) -> List[Pattern]:
        """
        Get all patterns for a specific entity type
        
        Args:
            entity_type: Entity type to get patterns for
            
        Returns:
            List[Pattern]: List of patterns for the entity type
        """
        return self._patterns.get(entity_type, [])
        
    def get_all_patterns(self) -> Dict[EntityType, List[Pattern]]:
        """
        Get all loaded patterns
        
        Returns:
            Dict[EntityType, List[Pattern]]: All patterns organized by type
        """
        return self._patterns.copy()
        
    def add_pattern(self, pattern: Pattern):
        """
        Add a new pattern to the matcher
        
        Args:
            pattern: Pattern to add
        """
        if pattern.entity_type not in self._patterns:
            self._patterns[pattern.entity_type] = []
            self._compiled_patterns[pattern.entity_type] = []
            
        self._patterns[pattern.entity_type].append(pattern)
        
        # Compile the new pattern
        if pattern.compiled_regex:
            self._compiled_patterns[pattern.entity_type].append(
                (pattern, pattern.compiled_regex)
            )
            
    def validate_patterns(self) -> Dict[str, Any]:
        """
        Validate all patterns by running their test cases
        
        Returns:
            Dict[str, Any]: Validation results
        """
        results = {
            'total_patterns': 0,
            'passed_patterns': 0,
            'failed_patterns': 0,
            'details': {}
        }
        
        for entity_type, patterns in self._patterns.items():
            type_results = []
            
            for pattern in patterns:
                test_results = pattern.test()
                pattern_result = {
                    'pattern_name': pattern.name,
                    'total_tests': len(test_results),
                    'passed_tests': sum(1 for r in test_results if r['passed']),
                    'test_details': test_results
                }
                pattern_result['passed'] = (pattern_result['passed_tests'] == 
                                          pattern_result['total_tests'])
                
                type_results.append(pattern_result)
                results['total_patterns'] += 1
                
                if pattern_result['passed']:
                    results['passed_patterns'] += 1
                else:
                    results['failed_patterns'] += 1
                    
            # Handle entity_type safely - could be enum or string
            if hasattr(entity_type, 'value'):
                entity_type_str = entity_type.value
            else:
                entity_type_str = str(entity_type)
                
            results['details'][entity_type_str] = type_results
            
        return results
        
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get pattern matcher statistics
        
        Returns:
            Dict[str, Any]: Statistics about loaded patterns
        """
        stats = {
            'pattern_directory': str(self.pattern_dir),
            'version_filter': self.version,
            'confidence_threshold': self.confidence_threshold,
            'entity_types': len(self._patterns),
            'total_patterns': sum(len(p) for p in self._patterns.values()),
            'patterns_by_type': {}
        }
        
        for entity_type, patterns in self._patterns.items():
            # Handle entity_type safely - could be enum or string
            if hasattr(entity_type, 'value'):
                entity_type_str = entity_type.value
            else:
                entity_type_str = str(entity_type)
                
            stats['patterns_by_type'][entity_type_str] = {
                'count': len(patterns),
                'avg_confidence': sum(p.confidence for p in patterns) / len(patterns) if patterns else 0,
                'min_confidence': min(p.confidence for p in patterns) if patterns else 0,
                'max_confidence': max(p.confidence for p in patterns) if patterns else 0
            }
            
        return stats