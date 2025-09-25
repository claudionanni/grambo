"""
Log parsing engine for entity extraction

This module provides the core log parsing functionality that coordinates
pattern matching and entity extraction from Galera log files.
"""

import sys
from pathlib import Path
from typing import List, Optional, Union, TextIO, Dict, Any
from datetime import datetime
import logging

from .entities import Entity, EntityType, create_default_registry
from .entities.session_manager import SessionManager
from .entities.relationships import RelationshipManager, RelationshipDiscovery
from .patterns import PatternMatcher


class LogParser:
    """
    Main log parser that coordinates entity extraction from log files
    
    This class manages the parsing pipeline, coordinating pattern matching,
    entity creation, and optional interactive learning.
    """
    
    def __init__(self, pattern_matcher: Optional[PatternMatcher] = None,
                 entity_registry=None, learning_mode: bool = False,
                 interactive: bool = False):
        """
        Initialize log parser
        
        Args:
            pattern_matcher: Pattern matcher instance (created if None)
            entity_registry: Entity registry (created if None)
            learning_mode: Enable learning mode for pattern improvement
            interactive: Enable interactive pattern validation
        """
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.pattern_matcher = pattern_matcher
        self.entity_registry = entity_registry or create_default_registry()
        self.session_manager = SessionManager()
        self.relationship_manager = RelationshipManager()
        self.relationship_discovery = RelationshipDiscovery(self.relationship_manager)
        self.learning_mode = learning_mode
        self.interactive = interactive
        
        # Parsing state
        self.current_line_number = 0
        self.current_file = ""
        
        # Statistics
        self.stats = {
            'total_lines': 0,
            'matched_lines': 0,
            'extracted_entities': 0,
            'entities_by_type': {},
            'patterns_used': set(),
            'parse_errors': 0,
            'learning_opportunities': 0
        }
        
        # Learning data (if enabled)
        self.learned_patterns = []
        self.validation_feedback = []
        
    def parse_file(self, file_path: Path, entity_types: Optional[List[EntityType]] = None) -> List[Entity]:
        """
        Parse a log file and extract entities
        
        Args:
            file_path: Path to log file
            entity_types: Optional list of entity types to extract
            
        Returns:
            List[Entity]: Extracted entities
        """
        self.current_file = str(file_path)
        self.current_line_number = 0
        
        self.logger.info(f"Parsing log file: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return self.parse_stream(f, entity_types)
        except FileNotFoundError:
            self.logger.error(f"Log file not found: {file_path}")
            raise
        except UnicodeDecodeError as e:
            self.logger.error(f"Encoding error reading {file_path}: {e}")
            # Try with different encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    return self.parse_stream(f, entity_types)
            except Exception as e2:
                self.logger.error(f"Failed to read file with any encoding: {e2}")
                raise
        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {e}")
            raise
            
    def parse_stream(self, stream: TextIO, entity_types: Optional[List[EntityType]] = None) -> List[Entity]:
        """
        Parse a stream and extract entities
        
        Args:
            stream: Input stream to parse
            entity_types: Optional list of entity types to extract
            
        Returns:
            List[Entity]: Extracted entities
        """
        entities = []
        self.current_line_number = 0
        
        if not self.pattern_matcher:
            self.logger.error("No pattern matcher configured")
            return entities
            
        try:
            for line in stream:
                self.current_line_number += 1
                self.stats['total_lines'] += 1
                
                # Skip empty lines
                line = line.rstrip('\n\r')
                if not line.strip():
                    continue
                    
                # Extract entities from line
                line_entities = self._parse_line(line, entity_types)
                
                if line_entities:
                    self.stats['matched_lines'] += 1
                    entities.extend(line_entities)
                    
                    # Update statistics
                    for entity in line_entities:
                        self.stats['extracted_entities'] += 1
                        # Handle entity_type safely - could be enum or string
                        if hasattr(entity.entity_type, 'value'):
                            entity_type = entity.entity_type.value
                        else:
                            entity_type = str(entity.entity_type)
                        self.stats['entities_by_type'][entity_type] = (
                            self.stats['entities_by_type'].get(entity_type, 0) + 1
                        )
                        self.stats['patterns_used'].add(entity.pattern_name)
                        
                elif self.learning_mode:
                    # Check if this line might be an entity we should learn
                    self._check_learning_opportunity(line)
                    
        except Exception as e:
            self.logger.error(f"Error parsing stream at line {self.current_line_number}: {e}")
            self.stats['parse_errors'] += 1
            
        self.logger.info(f"Parsing complete: {len(entities)} entities extracted from "
                        f"{self.stats['total_lines']} lines")
        
        # Discover relationships between entities
        if entities:
            self.logger.info("Discovering relationships between entities...")
            relationships_found = self.relationship_discovery.discover_all_relationships(entities)
            self.logger.info(f"Discovered {relationships_found} relationships")
        
        return entities
        
    def _parse_line(self, line: str, entity_types: Optional[List[EntityType]] = None) -> List[Entity]:
        """
        Parse a single log line and extract entities
        
        Args:
            line: Log line to parse
            entity_types: Optional list of entity types to extract
            
        Returns:
            List[Entity]: Entities extracted from the line
        """
        entities = []
        
        try:
            # Extract timestamp from line for session management
            timestamp = self._extract_timestamp(line)
            
            # First try pattern matching to extract any SST data
            sst_pattern_data = {}
            if self.pattern_matcher:
                matched_entities = self.pattern_matcher.match_line(line, entity_types)
                sst_pattern_entities = [e for e in matched_entities if e.entity_type == EntityType.STATE_TRANSFER]
                if sst_pattern_entities:
                    # Extract data from the first SST pattern match for session manager
                    sst_entity_dict = sst_pattern_entities[0].to_dict()
                    sst_pattern_data = {k: v for k, v in sst_entity_dict.items() 
                                       if k in ['donor_node', 'joiner_node', 'transfer_status', 'transfer_method']}
            
            # Try session management for SST events with pattern data
            sst_processed = False
            sst_entity = self.session_manager.process_sst_event(line, timestamp, sst_pattern_data)
            if sst_entity:
                entities.append(sst_entity)
                self.stats['extracted_entities'] += 1
                sst_processed = True
            else:
                # Check if session manager processed the line (even if it returned None)
                sst_processed = self.session_manager.sst_classifier.is_sst_related(line)
            
            # Use pattern matcher for non-SST entities or when session manager didn't process SST
            if self.pattern_matcher:
                matched_entities = self.pattern_matcher.match_line(line, entity_types)
                
                for entity in matched_entities:
                    # Skip SST entities if session manager already processed this line
                    if entity.entity_type == EntityType.STATE_TRANSFER and sst_processed:
                        continue
                        
                    # Enrich entity with parsing context
                    entity.line_number = self.current_line_number
                    entity.log_source = self.current_file
                    
                    # Validate entity
                    try:
                        if entity.validate():
                            entities.append(entity)
                            self.stats['extracted_entities'] += 1
                        else:
                            self.logger.warning(f"Entity validation failed at line {self.current_line_number}")
                    except Exception as e:
                        self.logger.warning(f"Entity validation error at line {self.current_line_number}: {e}")
                        # Still include entity but mark as unvalidated
                        if hasattr(entity, 'validation_notes'):
                            entity.validation_notes += f" Validation error: {e}"
                        entities.append(entity)
                        self.stats['extracted_entities'] += 1
                        
        except Exception as e:
            self.logger.error(f"Error parsing line {self.current_line_number}: {e}")
            self.stats['parse_errors'] = self.stats.get('parse_errors', 0) + 1
            
        # Interactive validation if enabled
        if self.interactive and entities:
            entities = self._interactive_validation(entities, line)
            
        return entities
    
    def _extract_timestamp(self, line: str) -> datetime:
        """
        Extract timestamp from log line.
        
        Args:
            line: Log line to extract timestamp from
            
        Returns:
            datetime: Extracted timestamp or current time if not found
        """
        import re
        
        # Common timestamp patterns for Galera logs
        patterns = [
            # MariaDB format: 2025-09-15 13:45:48
            r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',
            # MySQL format with microseconds: 2025-09-15T13:45:48.123456Z
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d*Z?)',
            # WSREP_SST format: (20250919 11:10:54.872)
            r'\((\d{8}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\)',
            # Syslog format: Sep 15 13:45:48
            r'([A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, line)
            if match:
                timestamp_str = match.group(1)
                try:
                    # Try different datetime parsing formats
                    if 'T' in timestamp_str:
                        # ISO format
                        timestamp_str = timestamp_str.rstrip('Z')
                        if '.' in timestamp_str:
                            return datetime.fromisoformat(timestamp_str)
                        else:
                            return datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S')
                    elif len(timestamp_str.split()) == 2 and len(timestamp_str.split()[0]) == 8:
                        # WSREP_SST format: 20250919 11:10:54.872 (YYYYMMDD HH:MM:SS.mmm)
                        if '.' in timestamp_str:
                            return datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S.%f')
                        else:
                            return datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S')
                    elif len(timestamp_str.split()) == 2:
                        # YYYY-MM-DD HH:MM:SS format
                        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    else:
                        # Syslog format - assume current year
                        current_year = datetime.now().year
                        return datetime.strptime(f"{current_year} {timestamp_str}", '%Y %b %d %H:%M:%S')
                except ValueError:
                    continue
        
        # If no timestamp found, use current time
        return datetime.now()
        
    def _check_learning_opportunity(self, line: str):
        """
        Check if an unmatched line represents a learning opportunity
        
        Args:
            line: Unmatched log line
        """
        # Simple heuristics to identify potential Galera log lines
        if self._looks_like_galera_line(line):
            self.stats['learning_opportunities'] += 1
            
            if self.interactive:
                self._interactive_learning(line)
                
    def _looks_like_galera_line(self, line: str) -> bool:
        """
        Use heuristics to determine if a line might be a Galera log entry
        
        Args:
            line: Log line to check
            
        Returns:
            bool: True if line looks like a Galera log entry
        """
        # Basic heuristics - look for common Galera keywords
        galera_keywords = [
            'WSREP', 'Galera', 'SST', 'IST', 'gcomm', 'wsrep_',
            'cluster', 'view', 'state change', 'seqno'
        ]
        
        line_upper = line.upper()
        return any(keyword.upper() in line_upper for keyword in galera_keywords)
        
    def _interactive_validation(self, entities: List[Entity], line: str) -> List[Entity]:
        """
        Interactively validate extracted entities with user
        
        Args:
            entities: Extracted entities
            line: Original log line
            
        Returns:
            List[Entity]: Validated entities
        """
        validated_entities = []
        
        print(f"\nLine {self.current_line_number}: {line}")
        print(f"Extracted {len(entities)} entities:")
        
        for i, entity in enumerate(entities):
            # Handle entity_type safely - could be enum or string
            if hasattr(entity.entity_type, 'value'):
                entity_type_str = entity.entity_type.value
            else:
                entity_type_str = str(entity.entity_type)
            print(f"\n{i+1}. {entity_type_str} (confidence: {entity.confidence:.2f})")
            print(f"   Pattern: {entity.pattern_name}")
            
            # Show key entity data
            entity_dict = entity.to_dict()
            key_fields = ['node_id', 'current_state', 'transfer_type', 'view_id', 'cluster_state']
            for field in key_fields:
                if field in entity_dict and entity_dict[field]:
                    print(f"   {field}: {entity_dict[field]}")
                    
            # Get user validation
            while True:
                response = input("   Valid? (y/n/s=skip): ").lower().strip()
                if response in ['y', 'yes']:
                    entity.mark_validated("User confirmed")
                    validated_entities.append(entity)
                    break
                elif response in ['n', 'no']:
                    confidence = input("   New confidence (0.0-1.0, or empty to discard): ").strip()
                    if confidence:
                        try:
                            new_conf = float(confidence)
                            entity.update_confidence(new_conf, "User adjusted")
                            entity.mark_validated("User adjusted confidence")
                            validated_entities.append(entity)
                        except ValueError:
                            print("   Invalid confidence value")
                            continue
                    break
                elif response in ['s', 'skip']:
                    break
                else:
                    print("   Please enter 'y', 'n', or 's'")
                    
        return validated_entities
        
    def _interactive_learning(self, line: str):
        """
        Interactive learning for unmatched lines
        
        Args:
            line: Unmatched log line
        """
        print(f"\nUnmatched line {self.current_line_number}: {line}")
        
        response = input("Should this be an entity? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            # Get entity type
            print("Entity types: " + ", ".join(t.value for t in EntityType))
            entity_type_str = input("Entity type: ").upper().strip()
            
            try:
                entity_type = EntityType(entity_type_str)
                
                # Get basic pattern info
                pattern_name = input("Pattern name: ").strip()
                if not pattern_name:
                    pattern_name = f"learned_{entity_type.value.lower()}_{len(self.learned_patterns) + 1}"
                    
                confidence = input("Confidence (0.0-1.0, default 0.7): ").strip()
                try:
                    confidence = float(confidence) if confidence else 0.7
                except ValueError:
                    confidence = 0.7
                    
                # Store learning data
                learning_data = {
                    'line': line,
                    'line_number': self.current_line_number,
                    'entity_type': entity_type,
                    'pattern_name': pattern_name,
                    'confidence': confidence,
                    'timestamp': datetime.now()
                }
                
                self.learned_patterns.append(learning_data)
                print(f"Learned pattern '{pattern_name}' for {entity_type.value}")
                
            except ValueError:
                print(f"Unknown entity type: {entity_type_str}")
                
    def save_learned_patterns(self, output_path: Path):
        """
        Save learned patterns to a file
        
        Args:
            output_path: Path to save learned patterns
        """
        if not self.learned_patterns:
            self.logger.info("No learned patterns to save")
            return
            
        # TODO: Implement pattern file generation from learned data
        # This would create new YAML pattern files based on learning
        self.logger.info(f"Saved {len(self.learned_patterns)} learned patterns to {output_path}")
        
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get parsing statistics
        
        Returns:
            Dict[str, Any]: Parsing statistics
        """
        stats = self.stats.copy()
        stats['patterns_used'] = list(stats['patterns_used'])
        
        # Calculate derived statistics
        if stats['total_lines'] > 0:
            stats['match_rate'] = stats['matched_lines'] / stats['total_lines']
        else:
            stats['match_rate'] = 0.0
            
        if stats['matched_lines'] > 0:
            stats['entities_per_matched_line'] = stats['extracted_entities'] / stats['matched_lines']
        else:
            stats['entities_per_matched_line'] = 0.0
            
        return stats
    
    def get_all_entities(self) -> List[Entity]:
        """
        Get all entities including those managed by session manager.
        
        Returns:
            List[Entity]: All entities from parsing and session management
        """
        all_entities = []
        
        # Add entities from session manager (temporal entities)
        all_entities.extend(self.session_manager.get_all_entities())
        
        return all_entities
    
    def get_session_statistics(self) -> Dict[str, Any]:
        """Get statistics from session manager."""
        return self.session_manager.get_session_statistics()
    
    def extract_timestamp(self, line: str) -> Optional[datetime]:
        """
        Extract timestamp from a log line.
        
        Args:
            line: Log line to parse timestamp from
            
        Returns:
            datetime object if timestamp found, None otherwise
        """
        import re
        
        # Common MariaDB timestamp patterns
        timestamp_patterns = [
            r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})',  # YYYY-MM-DD HH:MM:SS
            r'(\d{6}\s+\d{2}:\d{2}:\d{2})',               # YYMMDD HH:MM:SS  
            r'\((\d{8}\s+\d{2}:\d{2}:\d{2})\.\d+\)',      # (YYYYMMDD HH:MM:SS.mmm)
        ]
        
        for pattern in timestamp_patterns:
            match = re.search(pattern, line)
            if match:
                timestamp_str = match.group(1)
                try:
                    # Try different timestamp formats
                    if len(timestamp_str) == 19:  # YYYY-MM-DD HH:MM:SS
                        return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                    elif len(timestamp_str) == 15:  # YYMMDD HH:MM:SS
                        return datetime.strptime(timestamp_str, '%y%m%d %H:%M:%S')
                    elif len(timestamp_str) == 17:  # YYYYMMDD HH:MM:SS
                        return datetime.strptime(timestamp_str, '%Y%m%d %H:%M:%S')
                except ValueError:
                    continue
        
        # Default to current time if no timestamp found
        return datetime.now()
        
    def reset_statistics(self):
        """Reset parsing statistics"""
        self.stats = {
            'total_lines': 0,
            'matched_lines': 0,
            'extracted_entities': 0,
            'entities_by_type': {},
            'patterns_used': set(),
            'parse_errors': 0,
            'learning_opportunities': 0
        }