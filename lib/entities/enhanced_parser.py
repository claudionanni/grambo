"""
Enhanced Multi-Log Parser for Comprehensive Entity Extraction

This module provides the parsing infrastructure to extract all entity types
from single or multiple Galera log files and construct a comprehensive
ClusterEntity with full hierarchical analysis.

Key capabilities:
- Multi-log file processing with source tracking
- Pattern-based entity extraction using YAML configurations  
- Automatic entity correlation and relationship building
- Timeline reconstruction with conflict resolution
- Split-brain detection and cluster health analysis
"""

import os
import re
import yaml
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, TextIO
from pathlib import Path

from .base import EntityRegistry, Pattern, EntityType
from .core import register_core_entities
from .cluster import ClusterEntity
# Import temporal analyzer if available, otherwise create stub
try:
    from .temporal import TemporalAnalyzer
except ImportError:
    class TemporalAnalyzer:
        """Stub temporal analyzer for basic functionality"""
        def __init__(self):
            pass


logger = logging.getLogger(__name__)


class MultiLogParser:
    """
    Enhanced parser for extracting entities from single or multiple Galera log files
    
    This parser handles:
    - Multiple log file sources with proper tracking
    - Pattern-based entity extraction using YAML configurations
    - Automatic relationship building between entities
    - Timeline reconstruction and conflict resolution
    - Comprehensive cluster analysis
    """
    
    def __init__(self, pattern_dirs: Optional[List[str]] = None):
        """
        Initialize the multi-log parser
        
        Args:
            pattern_dirs: List of directories containing YAML pattern files
        """
        self.registry = EntityRegistry()
        self.patterns: Dict[str, Pattern] = {}
        self.temporal_analyzer = TemporalAnalyzer()
        
        # Register core entity types
        register_core_entities(self.registry)
        
        # Load patterns from directories
        if pattern_dirs:
            for pattern_dir in pattern_dirs:
                self.load_patterns_from_directory(pattern_dir)
        else:
            # Default pattern directory
            default_pattern_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                'patterns'
            )
            if os.path.exists(default_pattern_dir):
                self.load_patterns_from_directory(default_pattern_dir)
        
        logger.info(f"Initialized parser with {len(self.patterns)} patterns")
    
    def load_patterns_from_directory(self, pattern_dir: str):
        """
        Load all YAML pattern files from a directory
        
        Args:
            pattern_dir: Directory containing YAML pattern files
        """
        pattern_path = Path(pattern_dir)
        if not pattern_path.exists():
            logger.warning(f"Pattern directory not found: {pattern_dir}")
            return
        
        yaml_files = list(pattern_path.glob("*.yaml")) + list(pattern_path.glob("*.yml"))
        
        for yaml_file in yaml_files:
            try:
                self.load_patterns_from_file(str(yaml_file))
            except Exception as e:
                logger.error(f"Error loading patterns from {yaml_file}: {e}")
    
    def load_patterns_from_file(self, yaml_file: str):
        """
        Load patterns from a single YAML file
        
        Args:
            yaml_file: Path to YAML file containing pattern definitions
        """
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                pattern_data = yaml.safe_load(f)
            
            if not isinstance(pattern_data, dict) or 'patterns' not in pattern_data:
                logger.warning(f"Invalid pattern file format: {yaml_file}")
                return
            
            patterns_section = pattern_data['patterns']
            
            # Handle nested pattern structure (patterns organized by entity type)
            if isinstance(patterns_section, dict):
                total_loaded = 0
                for entity_type, pattern_list in patterns_section.items():
                    if isinstance(pattern_list, list):
                        for pattern_config in pattern_list:
                            try:
                                # Add entity type to pattern config
                                pattern_config = dict(pattern_config)  # Make a copy
                                pattern_config['entity_type'] = entity_type
                                
                                pattern = self._create_pattern_from_config(pattern_config)
                                self.patterns[pattern.name] = pattern
                                self.registry.register_pattern(pattern)
                                logger.debug(f"Loaded pattern: {pattern.name}")
                                total_loaded += 1
                                
                            except Exception as e:
                                logger.error(f"Error creating pattern from config: {e}")
                                logger.debug(f"Pattern config: {pattern_config}")
                
                logger.info(f"Loaded {total_loaded} patterns from {yaml_file}")
            
            # Handle flat pattern structure (list of patterns)
            elif isinstance(patterns_section, list):
                for pattern_config in patterns_section:
                    try:
                        pattern = self._create_pattern_from_config(pattern_config)
                        self.patterns[pattern.name] = pattern
                        self.registry.register_pattern(pattern)
                        logger.debug(f"Loaded pattern: {pattern.name}")
                        
                    except Exception as e:
                        logger.error(f"Error creating pattern from config: {e}")
                        logger.debug(f"Pattern config: {pattern_config}")
                
                logger.info(f"Loaded {len(patterns_section)} patterns from {yaml_file}")
            
            else:
                logger.warning(f"Unexpected patterns structure in {yaml_file}")
                return
            
        except Exception as e:
            logger.error(f"Error loading pattern file {yaml_file}: {e}")
    
    def _create_pattern_from_config(self, config: Dict[str, Any]) -> Pattern:
        """
        Create a Pattern object from YAML configuration
        
        Args:
            config: Pattern configuration dictionary
            
        Returns:
            Pattern object
        """
        # Map entity type string to enum
        entity_type_str = config.get('entity_type', 'NODE')
        
        # Handle string entity types by mapping to EntityType enum
        entity_type_mapping = {
            'NODE': EntityType.NODE,
            'NODE_STATE': EntityType.NODE_STATE,
            'CLUSTER': EntityType.CLUSTER,
            'SST': EntityType.STATE_TRANSFER,
            'STATE_TRANSFER': EntityType.STATE_TRANSFER,
            'VIEW': EntityType.VIEW,
            'WSREP_VIEW': EntityType.WSREP_VIEW,
            'COMMUNICATION': EntityType.COMMUNICATION,
            'WARNING': EntityType.WARNING,
            'ERROR': EntityType.ERROR,
            'PERFORMANCE': EntityType.PERFORMANCE,
            'TRANSACTION': EntityType.TRANSACTION,
        }
        
        entity_type = entity_type_mapping.get(entity_type_str, EntityType.NODE)
        
        pattern = Pattern(
            name=config['name'],
            entity_type=entity_type,
            version=config.get('version', '1.0'),
            regex=config.get('regex', ''),
            field_mappings=config.get('field_mappings', {}),
            required_fields=config.get('required_fields', []),
            description=config.get('description', ''),
            examples=config.get('examples', []),
            confidence=config.get('confidence', 0.8),
            test_cases=config.get('test_cases', [])
        )
        
        # Store the original config for multiline and other special processing
        # Using setattr to avoid type checker issues
        setattr(pattern, '_config', config)
        
        return pattern
    
    def parse_multiple_logs(self, log_files: List[Union[str, TextIO]], 
                           cluster_name: Optional[str] = None) -> ClusterEntity:
        """
        Parse multiple log files and create comprehensive cluster entity
        
        Args:
            log_files: List of log file paths or file objects
            cluster_name: Optional cluster name for identification
            
        Returns:
            ClusterEntity with complete analysis
        """
        cluster = ClusterEntity(
            cluster_name=cluster_name or "Unknown Cluster",
            analysis_start_time=datetime.now()
        )
        
        # Parse each log file
        for i, log_file in enumerate(log_files):
            try:
                log_source = self._get_log_source_name(log_file, i)
                entities = self.parse_single_log(log_file, log_source)
                
                # Add all entities to cluster
                for entity in entities:
                    cluster.add_entity(entity)
                
                logger.info(f"Parsed {len(entities)} entities from {log_source}")
                
            except Exception as e:
                logger.error(f"Error parsing log file {log_file}: {e}")
        
        # Finalize analysis
        cluster.analysis_end_time = datetime.now()
        cluster.validate()
        
        logger.info(f"Completed multi-log analysis: {cluster.get_cluster_summary()['entity_counts']['total_entities']} total entities")
        
        return cluster
    
    def parse_single_log(self, log_file: Union[str, TextIO], 
                        log_source: str = "unknown") -> List:
        """
        Parse a single log file and extract all entities
        
        Args:
            log_file: Log file path or file object
            log_source: Source identifier for the log
            
        Returns:
            List of extracted entities
        """
        entities = []
        
        try:
            # Handle both file paths and file objects
            if isinstance(log_file, str):
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                log_source = log_source if log_source != "unknown" else os.path.basename(log_file)
            else:
                lines = log_file.readlines()
                # Reset file pointer if possible
                try:
                    log_file.seek(0)
                except (AttributeError, OSError):
                    pass
            
            # First, try multiline patterns on the entire log content
            full_text = ''.join(lines)
            multiline_entities = self._extract_multiline_entities(full_text, log_source)
            entities.extend(multiline_entities)
            
            # Then parse each line for single-line patterns
            for line_number, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Try all single-line patterns against this line
                extracted_entities = self._extract_entities_from_line(
                    line, line_number, log_source
                )
                entities.extend(extracted_entities)
            
            logger.debug(f"Extracted {len(entities)} entities from {log_source}")
            
        except Exception as e:
            logger.error(f"Error parsing log {log_source}: {e}")
        
        return entities
    
    def _extract_entities_from_line(self, line: str, line_number: int, 
                                   log_source: str) -> List:
        """
        Extract entities from a single log line using all applicable patterns
        
        Args:
            line: Log line to parse
            line_number: Line number in the file
            log_source: Source identifier
            
        Returns:
            List of extracted entities
        """
        entities = []
        
        for pattern_name, pattern in self.patterns.items():
            try:
                # Check if pattern matches
                extracted_data = pattern.match(line)
                if extracted_data:
                    # Create entity from extracted data
                    entity = self._create_entity_from_extracted_data(
                        pattern, extracted_data, line, line_number, log_source
                    )
                    
                    if entity:
                        entities.append(entity)
                        logger.debug(f"Pattern '{pattern_name}' matched line {line_number}")
                    
            except Exception as e:
                logger.debug(f"Error applying pattern '{pattern_name}' to line {line_number}: {e}")
        
        # Update NODE entities with UUID information from WSREP views
        self._update_node_entities_with_uuids(entities)
        
        return entities
    
    def _extract_multiline_entities(self, full_text: str, log_source: str) -> List:
        """
        Extract entities from multiline patterns that span across multiple lines
        
        Args:
            full_text: Complete log file content as single string
            log_source: Source identifier
            
        Returns:
            List of extracted entities from multiline patterns
        """
        entities = []
        
        for pattern_name, pattern in self.patterns.items():
            try:
                # Check if this is a multiline pattern
                pattern_config = getattr(pattern, '_config', {})
                if not pattern_config.get('multiline', False):
                    continue
                
                # Apply multiline pattern to the full text
                regex_pattern = pattern_config.get('regex', '')
                if not regex_pattern:
                    continue
                
                # Use DOTALL flag to make . match newlines
                matches = re.finditer(regex_pattern, full_text, re.DOTALL | re.MULTILINE)
                
                for match in matches:
                    extracted_data = match.groupdict()
                    
                    # Find the line number for this match (approximate)
                    match_start = match.start()
                    lines_before = full_text[:match_start].count('\n') + 1
                    
                    # Create entity from extracted data
                    entity = self._create_entity_from_extracted_data(
                        pattern, extracted_data, match.group(0), lines_before, log_source
                    )
                    
                    if entity:
                        entities.append(entity)
                        logger.debug(f"Multiline pattern '{pattern_name}' matched at line {lines_before}")
                        
                        # Special processing for WSREP_VIEW to extract member information
                        if hasattr(entity, 'entity_type') and entity.entity_type.value == 'WSREP_VIEW':
                            self._process_wsrep_view_members(entity, extracted_data)
                    
            except Exception as e:
                logger.debug(f"Error applying multiline pattern '{pattern_name}': {e}")
        
        # Update NODE entities with UUID information from WSREP views
        self._update_node_entities_with_uuids(entities)
        
        return entities
    
    def _extract_short_uuid(self, full_uuid: str) -> str:
        """
        Extract short UUID from full UUID (parts 1 and 4)
        
        Args:
            full_uuid: Full UUID like "6d6256fd-9a23-11f0-b15e-83e09e5959f9"
            
        Returns:
            str: Short UUID like "6d6256fd-b15e"
        """
        try:
            # Split UUID by hyphens: ["6d6256fd", "9a23", "11f0", "b15e", "83e09e5959f9"]
            parts = full_uuid.split('-')
            if len(parts) >= 4:
                # Take part 1 (index 0) and part 4 (index 3)
                return f"{parts[0]}-{parts[3]}"
            else:
                # Fallback if UUID format is unexpected
                return full_uuid[:8] + "-" + full_uuid[-4:] if len(full_uuid) >= 12 else full_uuid
        except Exception:
            return full_uuid

    def _process_wsrep_view_members(self, entity, extracted_data: Dict[str, Any]):
        """
        Process WSREP view members section to extract individual member information
        
        Args:
            entity: WSREP view entity to update
            extracted_data: Raw extracted data containing members_section and other fields
        """
        try:
            # Set protocol_version
            protocol_version_raw = extracted_data.get('protocol_version', '')
            if protocol_version_raw:
                try:
                    entity.protocol_version = int(protocol_version_raw)
                except ValueError:
                    pass
            
            # Set own_index
            own_index_raw = extracted_data.get('own_index', '')
            if own_index_raw:
                try:
                    entity.own_index = int(own_index_raw)
                except ValueError:
                    pass
            
            # Set member_count
            member_count_raw = extracted_data.get('member_count', '')
            if member_count_raw:
                try:
                    entity.member_count = int(member_count_raw)
                except ValueError:
                    pass
            
            # Process members section
            members_section = extracted_data.get('members_section', '')
            if not members_section:
                return
            
            # Parse individual members and extract UUID information
            members = []
            member_uuids = []
            member_lines = [line.strip() for line in members_section.split('\n') if line.strip()]

            for line in member_lines:
                # Match pattern like "0: 378c0ec7-9236-11f0-a3db-f6fdc24ecc7d, UAT-DB-03"
                member_match = re.match(r'(\d+):\s+([a-f0-9-]+),\s+(.+)', line)
                if member_match:
                    index, full_uuid, name = member_match.groups()
                    short_uuid = self._extract_short_uuid(full_uuid)
                    
                    member_info = {
                        'index': index,
                        'uuid': full_uuid,
                        'short_uuid': short_uuid,
                        'name': name.strip()
                    }
                    members.append(member_info)
                    member_uuids.append(full_uuid)
                    
                    # Store UUID mapping for later NODE entity updates
                    if not hasattr(self, '_node_uuid_mappings'):
                        self._node_uuid_mappings = {}
                    
                    node_name = name.strip()
                    self._node_uuid_mappings[node_name] = {
                        'full_uuid': full_uuid,
                        'short_uuid': short_uuid,
                        'index': int(index) if index.isdigit() else None
                    }

            entity.members = members

            # Store member UUIDs as a separate list for easy access
            entity.member_uuids = member_uuids            # Update member_count if not already set from extracted data
            if entity.member_count is None:
                entity.member_count = len(members)
                
        except Exception as e:
            logger.debug(f"Error processing WSREP view members: {e}")
    
    def _update_node_entities_with_uuids(self, entities: List) -> None:
        """
        Update NODE entities with UUID information collected from WSREP views
        
        Args:
            entities: List of all entities extracted from the log
        """
        if not hasattr(self, '_node_uuid_mappings') or not self._node_uuid_mappings:
            return
            
        # Find all NODE entities and update them with UUID information
        for entity in entities:
            if entity.entity_type.value == 'NODE' and hasattr(entity, 'node_name') and entity.node_name:
                uuid_info = self._node_uuid_mappings.get(entity.node_name)
                if uuid_info:
                    # Update the node entity with UUID information
                    entity.short_uuid = uuid_info['short_uuid']
                    entity.node_uuid = uuid_info['full_uuid']
                    if uuid_info['index'] is not None:
                        entity.local_index = uuid_info['index']
                    
                    # Regenerate entity ID now that we have proper node identification
                    old_id = entity.entity_id
                    id_attrs = entity.get_id_attributes()
                    entity.entity_id = entity.generate_entity_id()
                    
                    logger.debug(f"Updated NODE entity {entity.node_name}: {old_id} -> {entity.entity_id}")
                    logger.debug(f"ID attributes: {id_attrs}")
                    logger.debug(f"UUID info: {uuid_info}")
    
    def _map_pattern_params_to_entity(self, entity_type: EntityType, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map pattern parameter names to entity constructor parameter names
        
        Args:
            entity_type: Type of entity being created
            extracted_data: Raw extracted data from pattern
            
        Returns:
            Mapped parameters suitable for entity constructor
        """
        from .base import EntityType
        
        # Parameter mapping for different entity types
        param_mappings = {
            EntityType.VIEW: {
                'from_state': 'before_state',  # ViewEntity expects 'before_state'
                'node_id': 'members',          # Single node_id becomes members list
                'member_id': 'members',        # member_id also becomes members list
                'state': 'cluster_state',      # state maps to cluster_state
            },
            EntityType.NODE: {
                'cluster_position': None,       # Ignore cluster_position for now (no direct mapping)
                'node_id': 'node_uuid',         # node_id maps to node_uuid
                'level': None,                  # Ignore level field for NODE entities
            },
            EntityType.NODE_STATE: {
                'from_state': 'from_state',     # Keep from_state as is
                'to_state': 'to_state',         # Keep to_state as is
                'level': None,                  # Ignore level field for NODE_STATE
            },
            EntityType.STATE_TRANSFER: {
                'role': None,                   # Ignore role field for now
                'level': None,                  # Ignore level field for STATE_TRANSFER
                'completed': None,              # Ignore completed field for now
                'transfer_method': 'transfer_method',  # Keep transfer_method as is
            },
            EntityType.WSREP_VIEW: {
                'capabilities_raw': None,       # Processed separately in _process_wsrep_view_members
                'members_section': None,        # Processed separately in _process_wsrep_view_members
                'final': None,                  # Processed separately in _process_wsrep_view_members
                'own_index': None,              # Processed separately in _process_wsrep_view_members
                'member_count': None,           # Processed separately in _process_wsrep_view_members
            }
        }
        
        # Get mapping for this entity type
        entity_mapping = param_mappings.get(entity_type, {})
        
        # Apply mappings
        mapped_data = {}
        for key, value in extracted_data.items():
            # Skip timestamp as it's handled separately
            if key == 'timestamp':
                continue
                
            # Map parameter name if needed
            mapped_key = entity_mapping.get(key, key)
            
            # Skip if mapped to None (means ignore this field)
            if mapped_key is None:
                continue
            
            # Special handling for certain mappings
            if key in ['node_id', 'member_id'] and entity_type == EntityType.VIEW:
                # Convert single ID to list for members
                mapped_data['members'] = [value] if value else []
            else:
                mapped_data[mapped_key] = value
        
        return mapped_data
    
    def _create_entity_from_extracted_data(self, pattern: Pattern, 
                                         extracted_data: Dict[str, Any],
                                         raw_line: str, line_number: int, 
                                         log_source: str):
        """
        Create an entity instance from extracted pattern data
        
        Args:
            pattern: Pattern that matched
            extracted_data: Data extracted by the pattern
            raw_line: Original log line
            line_number: Line number in file
            log_source: Source identifier
            
        Returns:
            Entity instance or None if creation failed
        """
        try:
            # Parse timestamp if available
            timestamp = self._parse_timestamp(extracted_data.get('timestamp', ''))
            
            # Prepare base entity fields
            base_fields = {
                'timestamp': timestamp,
                'line_number': line_number,
                'raw_line': raw_line,
                'log_source': log_source,
                'confidence': pattern.confidence,
                'pattern_name': pattern.name,
                'extraction_method': 'pattern'
            }
            
            # Map pattern parameters to entity constructor parameters
            mapped_data = self._map_pattern_params_to_entity(pattern.entity_type, extracted_data)
            
            # Add extracted fields
            entity_fields = {**base_fields, **mapped_data}
            
            # Special handling for NODE_STATE entities
            if pattern.entity_type == EntityType.NODE_STATE:
                # Provide default node reference if not present
                if 'node_uuid' not in entity_fields and 'node_reference' not in entity_fields:
                    # Use log source as default node reference for server status changes
                    entity_fields['node_reference'] = f"server_{log_source}"
                
                # Convert state strings to NodeState enum
                from .enhanced_nodes import NodeState
                for state_field in ['from_state', 'to_state']:
                    if state_field in entity_fields:
                        state_str = entity_fields[state_field].upper()
                        # Map common state strings to NodeState enum
                        state_mapping = {
                            'CONNECTED': NodeState.OPEN,
                            'INITIALIZED': NodeState.JOINED,
                            'DISCONNECTED': NodeState.CLOSED,
                            'SYNCED': NodeState.SYNCED,
                            'OPEN': NodeState.OPEN,
                            'JOINED': NodeState.JOINED,
                            'JOINER': NodeState.JOINER,
                            'DONOR': NodeState.DONOR,
                            'DESYNCED': NodeState.DESYNCED,
                            'CLOSED': NodeState.CLOSED
                        }
                        entity_fields[state_field] = state_mapping.get(state_str, NodeState.UNKNOWN)
            
            # Clean up None values and empty strings
            entity_fields = {k: v for k, v in entity_fields.items() 
                           if v is not None and v != ''}
            
            # Create entity using registry
            entity = self.registry.create_entity(pattern.entity_type, **entity_fields)
            
            if entity:
                return entity
            else:
                logger.debug(f"Failed to create entity of type {pattern.entity_type}")
                return None
                
        except Exception as e:
            logger.debug(f"Error creating entity from extracted data: {e}")
            logger.debug(f"Extracted data: {extracted_data}")
            return None
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string into datetime object
        
        Args:
            timestamp_str: Timestamp string from log
            
        Returns:
            datetime object or None if parsing failed
        """
        if not timestamp_str:
            return None
        
        # Common Galera timestamp formats
        timestamp_formats = [
            '%Y-%m-%d %H:%M:%S',           # 2024-01-15 10:30:45
            '%Y-%m-%dT%H:%M:%S',           # 2024-01-15T10:30:45
            '%Y-%m-%dT%H:%M:%S.%f',        # 2024-01-15T10:30:45.123456
            '%Y-%m-%d %H:%M:%S.%f',        # 2024-01-15 10:30:45.123456
            '%b %d %H:%M:%S',              # Jan 15 10:30:45
            '%Y%m%d %H:%M:%S',             # 20240115 10:30:45
        ]
        
        for fmt in timestamp_formats:
            try:
                return datetime.strptime(timestamp_str.strip(), fmt)
            except ValueError:
                continue
        
        # Try to extract timestamp using regex if standard formats fail
        timestamp_patterns = [
            r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?',
            r'\d{4}\d{2}\d{2} \d{2}:\d{2}:\d{2}',
            r'[A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2}',
        ]
        
        for pattern in timestamp_patterns:
            match = re.search(pattern, timestamp_str)
            if match:
                try:
                    # Try parsing the matched substring
                    matched_str = match.group(0)
                    for fmt in timestamp_formats:
                        try:
                            return datetime.strptime(matched_str, fmt)
                        except ValueError:
                            continue
                except Exception:
                    continue
        
        logger.debug(f"Could not parse timestamp: {timestamp_str}")
        return None
    
    def _get_log_source_name(self, log_file: Union[str, TextIO], index: int) -> str:
        """
        Get a descriptive name for the log source
        
        Args:
            log_file: Log file path or file object
            index: Index in the list of log files
            
        Returns:
            Descriptive log source name
        """
        if isinstance(log_file, str):
            return os.path.basename(log_file)
        else:
            # Try to get name from file object
            if hasattr(log_file, 'name'):
                return os.path.basename(log_file.name)
            else:
                return f"log_stream_{index}"
    
    def validate_patterns(self) -> Dict[str, Any]:
        """
        Validate all loaded patterns by running their test cases
        
        Returns:
            Validation results for all patterns
        """
        results = {}
        
        for pattern_name, pattern in self.patterns.items():
            try:
                test_results = pattern.test()
                
                results[pattern_name] = {
                    'total_tests': len(test_results),
                    'passed_tests': sum(1 for r in test_results if r['passed']),
                    'failed_tests': sum(1 for r in test_results if not r['passed']),
                    'success_rate': (sum(1 for r in test_results if r['passed']) / len(test_results)) if test_results else 0,
                    'details': test_results
                }
                
            except Exception as e:
                results[pattern_name] = {
                    'error': str(e),
                    'total_tests': 0,
                    'passed_tests': 0,
                    'failed_tests': 0,
                    'success_rate': 0
                }
        
        return results
    
    def get_parser_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive parser statistics
        
        Returns:
            Dictionary with parser statistics
        """
        return {
            'total_patterns': len(self.patterns),
            'patterns_by_entity_type': {
                entity_type.value: len([p for p in self.patterns.values() 
                                      if p.entity_type == entity_type])
                for entity_type in EntityType
            },
            'registered_entity_classes': len(self.registry._entity_classes),
            'pattern_names': list(self.patterns.keys())
        }


def create_enhanced_parser(pattern_dirs: Optional[List[str]] = None) -> MultiLogParser:
    """
    Factory function to create a configured MultiLogParser
    
    Args:
        pattern_dirs: Optional list of pattern directories
        
    Returns:
        Configured MultiLogParser instance
    """
    return MultiLogParser(pattern_dirs=pattern_dirs)


# Convenience functions for common use cases

def parse_galera_logs(log_files: List[Union[str, TextIO]], 
                     cluster_name: Optional[str] = None,
                     pattern_dirs: Optional[List[str]] = None) -> ClusterEntity:
    """
    Convenience function to parse Galera logs and return cluster analysis
    
    Args:
        log_files: List of log file paths or file objects
        cluster_name: Optional cluster name
        pattern_dirs: Optional pattern directories
        
    Returns:
        ClusterEntity with complete analysis
    """
    parser = create_enhanced_parser(pattern_dirs)
    return parser.parse_multiple_logs(log_files, cluster_name)


def analyze_cluster_from_directory(log_directory: str, 
                                  cluster_name: Optional[str] = None,
                                  log_pattern: str = "*.log") -> ClusterEntity:
    """
    Analyze all log files in a directory
    
    Args:
        log_directory: Directory containing log files
        cluster_name: Optional cluster name
        log_pattern: Glob pattern for log files (default: "*.log")
        
    Returns:
        ClusterEntity with complete analysis
    """
    log_dir_path = Path(log_directory)
    if not log_dir_path.exists():
        raise ValueError(f"Log directory not found: {log_directory}")
    
    log_files = list(log_dir_path.glob(log_pattern))
    if not log_files:
        raise ValueError(f"No log files found in {log_directory} matching pattern {log_pattern}")
    
    log_file_paths = [str(f) for f in log_files]
    return parse_galera_logs(log_file_paths, cluster_name)