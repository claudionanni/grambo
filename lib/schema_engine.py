"""
Schema-driven entity extraction engine

This module implements a deterministic entity extraction system based on
structured schema definitions for entities and patterns.
"""

import re
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import logging


class EntityCategory(Enum):
    """Categories of entities"""
    CORE = "core"  # Immutable base entities
    TEMPORAL = "temporal"  # Time-based events


class ContextType(Enum):
    """Pattern context types"""
    LOCAL = "LOCAL"  # Local node event
    GLOBAL = "GLOBAL"  # Cluster-wide information
    PEER = "PEER"  # Information about peer nodes


class ActionType(Enum):
    """Entity actions"""
    CREATE_CORE = "CREATE_CORE"
    UPDATE_CORE = "UPDATE_CORE"
    CREATE_TEMPORAL = "CREATE_TEMPORAL"
    UPDATE_TEMPORAL = "UPDATE_TEMPORAL"


@dataclass
class EntitySchema:
    """Schema definition for an entity type"""
    name: str
    category: EntityCategory
    description: str
    immutable: bool
    unique_keys: List[Union[str, List[str]]]
    attributes: Dict[str, Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    time_series: bool = False
    
    def validate_data(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate data against schema"""
        errors = []
        
        # Check required fields
        for attr_name, attr_def in self.attributes.items():
            if attr_def.get('required', False) and attr_name not in data:
                errors.append(f"Missing required field: {attr_name}")
        
        # Validate field patterns
        for attr_name, value in data.items():
            if attr_name in self.attributes:
                attr_def = self.attributes[attr_name]
                pattern = attr_def.get('pattern')
                if pattern and value:
                    if not re.match(pattern, str(value)):
                        errors.append(f"Field {attr_name} does not match pattern: {pattern}")
        
        return len(errors) == 0, errors


@dataclass
class ExtractionMapping:
    """Defines how to extract data from regex match"""
    match_group: Optional[str] = None
    extraction_fn: str = "direct_mapping"
    transform: Optional[str] = None
    target_field: str = ""
    computed: bool = False
    source: Optional[str] = None
    constant: Optional[Any] = None


@dataclass
class EntityAction:
    """Defines what to do with extracted data"""
    action_type: ActionType
    entity_type: str
    parent_refs: List[Dict[str, Any]] = field(default_factory=list)
    unique_keys: List[str] = field(default_factory=list)
    lookup_keys: List[str] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    computed_fields: Dict[str, str] = field(default_factory=dict)


@dataclass
class Pattern:
    """Complete pattern definition"""
    pattern_id: str
    entity_target: str
    regex: str
    context: ContextType
    confidence: float
    extraction_mapping: List[ExtractionMapping]
    entity_action: EntityAction
    description: str = ""
    validation_rules: List[Dict[str, Any]] = field(default_factory=list)
    examples: List[Dict[str, Any]] = field(default_factory=list)
    
    # Compiled regex
    _compiled: Optional[re.Pattern] = field(default=None, init=False, repr=False)
    
    def compile(self):
        """Compile regex pattern"""
        self._compiled = re.compile(self.regex)
    
    def match(self, line: str) -> Optional[re.Match]:
        """Match line against pattern"""
        if not self._compiled:
            self.compile()
        return self._compiled.search(line)


class SchemaLoader:
    """Loads and manages entity and pattern schemas"""
    
    def __init__(self, schema_dir: Path):
        self.schema_dir = Path(schema_dir)
        self.logger = logging.getLogger(__name__)
        
        # Storage
        self.entity_schemas: Dict[str, EntitySchema] = {}
        self.patterns: List[Pattern] = []
        self.patterns_by_entity: Dict[str, List[Pattern]] = {}
        
        # Load schemas
        self._load_entity_schema()
        self._load_patterns()
    
    def _load_entity_schema(self):
        """Load entity schema from YAML"""
        schema_file = self.schema_dir / "entity_schema.yaml"
        
        if not schema_file.exists():
            self.logger.error(f"Entity schema not found: {schema_file}")
            return
        
        with open(schema_file, 'r') as f:
            schema_data = yaml.safe_load(f)
        
        # Load CORE entities
        for entity_name, entity_def in schema_data.get('core_entities', {}).items():
            schema = EntitySchema(
                name=entity_name,
                category=EntityCategory.CORE,
                description=entity_def.get('description', ''),
                immutable=entity_def.get('immutable', True),
                unique_keys=entity_def.get('unique_keys', []),
                attributes=entity_def.get('attributes', {}),
                relationships=entity_def.get('relationships', []),
                time_series=False
            )
            self.entity_schemas[entity_name] = schema
        
        # Load TEMPORAL entities
        for entity_name, entity_def in schema_data.get('temporal_entities', {}).items():
            schema = EntitySchema(
                name=entity_name,
                category=EntityCategory.TEMPORAL,
                description=entity_def.get('description', ''),
                immutable=False,
                unique_keys=[],  # Temporal entities don't have unique keys
                attributes=entity_def.get('attributes', {}),
                relationships=entity_def.get('relationships', []),
                time_series=entity_def.get('time_series', True)
            )
            self.entity_schemas[entity_name] = schema
        
        self.logger.info(f"Loaded {len(self.entity_schemas)} entity schemas")
    
    def _load_patterns(self):
        """Load pattern definitions from YAML"""
        patterns_file = self.schema_dir / "patterns.yaml"
        
        if not patterns_file.exists():
            self.logger.error(f"Patterns file not found: {patterns_file}")
            return
        
        with open(patterns_file, 'r') as f:
            patterns_data = yaml.safe_load(f)
        
        # Load core patterns
        for pattern_def in patterns_data.get('core_patterns', []):
            pattern = self._parse_pattern(pattern_def)
            if pattern:
                self.patterns.append(pattern)
                
                # Index by entity
                entity_type = pattern.entity_target
                if entity_type not in self.patterns_by_entity:
                    self.patterns_by_entity[entity_type] = []
                self.patterns_by_entity[entity_type].append(pattern)
        
        # Load temporal patterns
        for pattern_def in patterns_data.get('temporal_patterns', []):
            pattern = self._parse_pattern(pattern_def)
            if pattern:
                self.patterns.append(pattern)
                
                # Index by entity
                entity_type = pattern.entity_target
                if entity_type not in self.patterns_by_entity:
                    self.patterns_by_entity[entity_type] = []
                self.patterns_by_entity[entity_type].append(pattern)
        
        # Sort patterns by confidence (highest first)
        self.patterns.sort(key=lambda p: p.confidence, reverse=True)
        
        self.logger.info(f"Loaded {len(self.patterns)} patterns")
    
    def _parse_pattern(self, pattern_def: Dict[str, Any]) -> Optional[Pattern]:
        """Parse pattern definition into Pattern object"""
        try:
            # Parse extraction mappings
            extraction_mappings = []
            for mapping_def in pattern_def.get('extraction_mapping', []):
                mapping = ExtractionMapping(
                    match_group=mapping_def.get('match_group'),
                    extraction_fn=mapping_def.get('extraction_fn', 'direct_mapping'),
                    transform=mapping_def.get('transform'),
                    target_field=mapping_def.get('target_field', ''),
                    computed=mapping_def.get('computed', False),
                    source=mapping_def.get('source'),
                    constant=mapping_def.get('constant')
                )
                extraction_mappings.append(mapping)
            
            # Parse entity action
            action_def = pattern_def.get('entity_action', {})
            entity_action = EntityAction(
                action_type=ActionType[action_def.get('action_type', 'CREATE_TEMPORAL')],
                entity_type=action_def.get('entity_type', ''),
                parent_refs=action_def.get('parent_refs', []),
                unique_keys=action_def.get('unique_keys', []),
                lookup_keys=action_def.get('lookup_keys', []),
                attributes=action_def.get('attributes', {}),
                computed_fields=action_def.get('computed_fields', {})
            )
            
            # Create pattern
            pattern = Pattern(
                pattern_id=pattern_def['pattern_id'],
                entity_target=pattern_def['entity_target'],
                regex=pattern_def['regex'],
                context=ContextType[pattern_def.get('context', 'LOCAL')],
                confidence=pattern_def.get('confidence', 0.8),
                extraction_mapping=extraction_mappings,
                entity_action=entity_action,
                description=pattern_def.get('description', ''),
                validation_rules=pattern_def.get('validation_rules', []),
                examples=pattern_def.get('examples', [])
            )
            
            # Compile regex
            pattern.compile()
            
            return pattern
            
        except Exception as e:
            self.logger.error(f"Failed to parse pattern {pattern_def.get('pattern_id', 'unknown')}: {e}")
            return None
    
    def get_entity_schema(self, entity_name: str) -> Optional[EntitySchema]:
        """Get schema for entity type"""
        return self.entity_schemas.get(entity_name)
    
    def get_patterns_for_entity(self, entity_name: str) -> List[Pattern]:
        """Get all patterns that extract this entity type"""
        return self.patterns_by_entity.get(entity_name, [])


class DataExtractor:
    """Handles data extraction and transformation"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._transform_functions = self._register_transforms()
    
    def _register_transforms(self) -> Dict[str, Callable]:
        """Register transformation functions"""
        return {
            'parse_datetime': self._parse_datetime,
            'parse_int': self._parse_int,
            'parse_float': self._parse_float,
            'parse_boolean': self._parse_boolean,
            'to_uppercase': lambda x: x.upper() if x else x,
            'to_lowercase': lambda x: x.lower() if x else x,
            'normalize_state': self._normalize_state,
            'extract_uuid': self._extract_uuid,
            'extract_seqno_from_view_id': self._extract_seqno_from_view_id,
        }
    
    def extract_data(self, pattern: Pattern, match: re.Match, 
                    context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from regex match using pattern's extraction mapping"""
        extracted = {}
        
        for mapping in pattern.extraction_mapping:
            try:
                value = self._extract_field(mapping, match, context)
                if value is not None:
                    extracted[mapping.target_field] = value
            except Exception as e:
                self.logger.warning(f"Failed to extract field {mapping.target_field}: {e}")
        
        return extracted
    
    def _extract_field(self, mapping: ExtractionMapping, match: re.Match,
                      context: Dict[str, Any]) -> Any:
        """Extract single field value"""
        
        # Handle constant values
        if mapping.constant is not None:
            return mapping.constant
        
        # Handle computed values
        if mapping.computed:
            return context.get(mapping.source)
        
        # Handle match group extraction
        if mapping.match_group:
            value = match.group(mapping.match_group)
            
            # Apply transformation if specified
            if mapping.transform and value:
                transform_fn = self._transform_functions.get(mapping.transform)
                if transform_fn:
                    value = transform_fn(value)
            
            return value
        
        return None
    
    def _parse_datetime(self, value: str) -> datetime:
        """Parse datetime string"""
        # Common Galera log format: 2024-09-15 10:30:45
        try:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # Try ISO format
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return datetime.now()
    
    def _parse_int(self, value: str) -> int:
        """Parse integer"""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0
    
    def _parse_float(self, value: str) -> float:
        """Parse float"""
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean"""
        return value.lower() in ('true', 'yes', '1', 'on')
    
    def _normalize_state(self, value: str) -> str:
        """Normalize Galera state names"""
        # Handle compound states like "Donor/Desynced"
        if '/' in value:
            value = value.split('/')[0]
        return value.upper()
    
    def _extract_uuid(self, value: str) -> str:
        """Extract UUID from string"""
        uuid_pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
        match = re.search(uuid_pattern, value)
        return match.group(0) if match else value
    
    def _extract_seqno_from_view_id(self, view_id: str) -> int:
        """Extract sequence number from view_id (format: uuid:seqno)"""
        try:
            return int(view_id.split(':')[1])
        except (IndexError, ValueError):
            return 0


class EntityStore:
    """Stores and manages entities"""
    
    def __init__(self, schema_loader: SchemaLoader):
        self.schema_loader = schema_loader
        self.logger = logging.getLogger(__name__)
        
        # Storage: {entity_type: {entity_id: entity_data}}
        self.core_entities: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.temporal_entities: Dict[str, List[Dict[str, Any]]] = {}
    
    def create_or_update_core_entity(self, entity_type: str, data: Dict[str, Any]) -> Tuple[str, bool]:
        """
        Create or update CORE entity
        
        Returns:
            Tuple of (entity_id, created) where created is True if new entity was created
        """
        schema = self.schema_loader.get_entity_schema(entity_type)
        if not schema:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        if schema.category != EntityCategory.CORE:
            raise ValueError(f"Entity {entity_type} is not a CORE entity")
        
        # Validate data
        valid, errors = schema.validate_data(data)
        if not valid:
            self.logger.warning(f"Validation errors for {entity_type}: {errors}")
        
        # Check if entity exists
        if entity_type not in self.core_entities:
            self.core_entities[entity_type] = {}
        
        entity_id = self._find_or_create_entity_id(entity_type, schema, data)
        
        if entity_id in self.core_entities[entity_type]:
            # Update existing entity (enrich with new data)
            existing = self.core_entities[entity_type][entity_id]
            for key, value in data.items():
                if value and not existing.get(key):
                    existing[key] = value
            return entity_id, False
        else:
            # Create new entity
            data['entity_id'] = entity_id
            data['entity_type'] = entity_type
            self.core_entities[entity_type][entity_id] = data
            return entity_id, True
    
    def create_temporal_entity(self, entity_type: str, data: Dict[str, Any]) -> str:
        """Create TEMPORAL entity"""
        schema = self.schema_loader.get_entity_schema(entity_type)
        if not schema:
            raise ValueError(f"Unknown entity type: {entity_type}")
        
        if schema.category != EntityCategory.TEMPORAL:
            raise ValueError(f"Entity {entity_type} is not a TEMPORAL entity")
        
        # Validate data
        valid, errors = schema.validate_data(data)
        if not valid:
            self.logger.warning(f"Validation errors for {entity_type}: {errors}")
        
        # Generate entity ID
        import uuid
        entity_id = f"{entity_type}_{uuid.uuid4().hex[:12]}"
        
        data['entity_id'] = entity_id
        data['entity_type'] = entity_type
        
        # Store temporal entity
        if entity_type not in self.temporal_entities:
            self.temporal_entities[entity_type] = []
        self.temporal_entities[entity_type].append(data)
        
        return entity_id
    
    def _find_or_create_entity_id(self, entity_type: str, schema: EntitySchema, 
                                   data: Dict[str, Any]) -> str:
        """Find existing entity or generate new ID"""
        # Try to find existing entity by unique keys
        for unique_key in schema.unique_keys:
            if isinstance(unique_key, list):
                # Composite key
                key_values = [str(data.get(k, '')) for k in unique_key]
                if all(key_values):
                    entity_id = f"{entity_type}_{':'.join(key_values)}"
                    if entity_id in self.core_entities.get(entity_type, {}):
                        return entity_id
            else:
                # Single key
                key_value = data.get(unique_key)
                if key_value:
                    entity_id = f"{entity_type}_{key_value}"
                    if entity_id in self.core_entities.get(entity_type, {}):
                        return entity_id
        
        # Generate new ID from first unique key
        if schema.unique_keys:
            unique_key = schema.unique_keys[0]
            if isinstance(unique_key, list):
                key_values = [str(data.get(k, '')) for k in unique_key]
                return f"{entity_type}_{':'.join(key_values)}"
            else:
                key_value = data.get(unique_key, 'unknown')
                return f"{entity_type}_{key_value}"
        
        # Fallback: generate UUID
        import uuid
        return f"{entity_type}_{uuid.uuid4().hex[:12]}"
    
    def get_all_entities(self) -> Dict[str, Any]:
        """Get all entities for export"""
        return {
            'core_entities': self.core_entities,
            'temporal_entities': self.temporal_entities
        }


class SchemaBasedExtractor:
    """Main extractor that uses schema-driven patterns"""
    
    def __init__(self, schema_dir: Path):
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.schema_loader = SchemaLoader(schema_dir)
        self.data_extractor = DataExtractor()
        self.entity_store = EntityStore(self.schema_loader)
        
        # Processing context
        self.current_context = {}
    
    def process_log_file(self, log_file: Path) -> Dict[str, Any]:
        """Process log file and extract entities"""
        self.logger.info(f"Processing log file: {log_file}")
        
        # Update context
        self.current_context = {
            'log_file_path': str(log_file),
            'local_node_uuid': None,  # Will be discovered
            'cluster_uuid': None  # Will be discovered
        }
        
        line_number = 0
        matched_count = 0
        
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line_number += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    # Update context
                    self.current_context['current_line'] = line
                    self.current_context['line_number'] = line_number
                    
                    # Try to match against patterns
                    if self._match_and_extract(line):
                        matched_count += 1
        
        except Exception as e:
            self.logger.error(f"Error processing {log_file}: {e}")
        
        self.logger.info(f"Processed {line_number} lines, matched {matched_count}")
        
        # Return all entities
        return self.entity_store.get_all_entities()
    
    def _match_and_extract(self, line: str) -> bool:
        """Try to match line against patterns and extract entities"""
        
        # Try patterns in order (sorted by confidence)
        for pattern in self.schema_loader.patterns:
            match = pattern.match(line)
            if match:
                try:
                    self._process_match(pattern, match)
                    return True
                except Exception as e:
                    self.logger.error(f"Error processing pattern {pattern.pattern_id}: {e}")
                    continue
        
        return False
    
    def _process_match(self, pattern: Pattern, match: re.Match):
        """Process a pattern match and create/update entities"""
        
        # Extract data using pattern's extraction mapping
        extracted_data = self.data_extractor.extract_data(pattern, match, self.current_context)
        
        # Execute entity action
        action = pattern.entity_action
        
        if action.action_type == ActionType.CREATE_CORE:
            # Create or update CORE entity
            entity_id, created = self.entity_store.create_or_update_core_entity(
                action.entity_type,
                extracted_data
            )
            
            # Update context if this defines local node or cluster
            if action.entity_type == 'Node' and pattern.context == ContextType.LOCAL:
                self.current_context['local_node_uuid'] = extracted_data.get('node_uuid')
            if action.entity_type == 'Cluster':
                self.current_context['cluster_uuid'] = extracted_data.get('cluster_uuid')
            
            self.logger.debug(f"{'Created' if created else 'Updated'} {action.entity_type}: {entity_id}")
        
        elif action.action_type == ActionType.CREATE_TEMPORAL:
            # Handle parent references
            for parent_ref in action.parent_refs:
                parent_entity_type = parent_ref.get('entity_type')
                foreign_key = parent_ref.get('foreign_key')
                source_field = parent_ref.get('source_field')
                auto_create = parent_ref.get('auto_create_parent', False)
                
                if source_field in extracted_data:
                    extracted_data[foreign_key] = extracted_data[source_field]
                elif foreign_key in self.current_context:
                    extracted_data[foreign_key] = self.current_context[foreign_key]
                
                # Auto-create parent if needed
                if auto_create and foreign_key in extracted_data:
                    parent_attrs = parent_ref.get('parent_attributes', {})
                    # Substitute placeholders
                    for key, value in parent_attrs.items():
                        if isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                            field_name = value[1:-1]
                            parent_attrs[key] = extracted_data.get(field_name, self.current_context.get(field_name))
                    
                    self.entity_store.create_or_update_core_entity(parent_entity_type, parent_attrs)
            
            # Create temporal entity
            entity_id = self.entity_store.create_temporal_entity(action.entity_type, extracted_data)
            self.logger.debug(f"Created {action.entity_type}: {entity_id}")


def main():
    """Example usage"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize extractor
    schema_dir = Path(__file__).parent / "schema"
    extractor = SchemaBasedExtractor(schema_dir)
    
    # Process log file
    log_file = Path("test.log")
    if log_file.exists():
        entities = extractor.process_log_file(log_file)
        
        # Export to JSON
        import json
        output_file = Path("entities_output.json")
        with open(output_file, 'w') as f:
            json.dump(entities, f, indent=2, default=str)
        
        print(f"Extracted entities saved to {output_file}")


if __name__ == "__main__":
    main()
