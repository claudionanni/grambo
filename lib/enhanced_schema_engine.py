"""
Enhanced Schema-driven entity extraction engine for v3-alpha
Implements missing NodeStateChange, ClusterView, and QuorumEvent patterns
Maintains compatibility with graf frame-based state machine
"""

import re
import yaml
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import logging

# Import base schema engine
from lib.schema_engine import (
    EntityCategory, ContextType, ActionType,
    EntitySchema, ExtractionMapping, EntityAction, Pattern,
    SchemaLoader, DataExtractor, EntityStore
)


class EnhancedDataExtractor(DataExtractor):
    """
    Enhanced extractor with support for complex temporal patterns
    and timestamp indexing for frame generation
    """
    
    def __init__(self, schema_loader: 'SchemaLoader'):
        super().__init__(schema_loader)
        self.local_node_name = None
        self.local_node_uuid = None
        self.current_file = None
        self.line_number = 0
        
        # Timestamp indexing for frame generation
        self.timestamp_indices = defaultdict(int)
        
    def set_context(self, node_name: str = None, node_uuid: str = None, file_path: str = None):
        """Set extraction context"""
        if node_name:
            self.local_node_name = node_name
        if node_uuid:
            self.local_node_uuid = node_uuid
        if file_path:
            self.current_file = file_path
    
    def extract_from_line(self, line: str, line_num: int, 
                         context: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Extract entities from a single log line with timestamp indexing
        
        Returns list of extracted entities with timestamp_index for frame ordering
        """
        self.line_number = line_num
        
        if context is None:
            context = {}
        
        # Add current context
        context.update({
            'log_file_path': self.current_file,
            'line_number': line_num,
            'current_line': line,
            'local_node_name': self.local_node_name,
            'local_node_uuid': self.local_node_uuid,
        })
        
        extracted_entities = []
        
        # Try each pattern
        for pattern in self.patterns:
            match = pattern.compiled_regex.match(line)
            if not match:
                continue
            
            # Extract data using base class method
            data = self._extract_data_from_match(match, pattern, context)
            if not data:
                continue
            
            # Add timestamp index for frame ordering
            timestamp = data.get('timestamp')
            if timestamp:
                # Convert to string if datetime
                if isinstance(timestamp, datetime):
                    timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    timestamp_str = str(timestamp)
                
                # Increment index for this timestamp
                data['timestamp_index'] = self.timestamp_indices[timestamp_str]
                self.timestamp_indices[timestamp_str] += 1
                data['timestamp_str'] = timestamp_str
            else:
                data['timestamp_index'] = 0
            
            extracted_entities.append(data)
        
        return extracted_entities
    
    def _transform_normalize_state(self, value: str) -> Optional[str]:
        """Normalize Galera state names"""
        if not value:
            return None
        
        state = value.strip().upper()
        
        # Canonical states from Galera
        canonical = {
            'CLOSED', 'OPEN', 'PRIMARY', 
            'JOINER', 'JOINED', 'SYNCED', 
            'DONOR', 'DONOR/DESYNCED', 'DESYNCED',
            'DESTROYED'
        }
        
        if state in canonical:
            return state
        
        # Handle variations
        if state == 'DONOR/DESYNCED' or state == 'DONOR':
            return 'DONOR/DESYNCED'
        
        return None
    
    def _transform_extract_seqno_from_view_id(self, view_id: str) -> Optional[int]:
        """Extract sequence number from view_id format: uuid:seqno"""
        if ':' in view_id:
            try:
                return int(view_id.split(':')[1])
            except (ValueError, IndexError):
                pass
        return None
    
    def _transform_uuid_part_of_view_id(self, view_id: str) -> Optional[str]:
        """Extract UUID from view_id format: uuid:seqno"""
        if ':' in view_id:
            return view_id.split(':')[0]
        return None


class EnhancedEntityStore(EntityStore):
    """
    Enhanced entity store with frame-compatible output
    Maintains chronological ordering with timestamp indices
    """
    
    def __init__(self):
        super().__init__()
        self.temporal_sequence = []  # List of (timestamp, index, entity_ref)
    
    def add_temporal_entity(self, entity_type: str, entity_data: Dict[str, Any]) -> str:
        """Add temporal entity and track for frame generation"""
        entity_id = super().add_temporal_entity(entity_type, entity_data)
        
        # Track in sequence for frame generation
        timestamp = entity_data.get('timestamp')
        timestamp_index = entity_data.get('timestamp_index', 0)
        
        if timestamp:
            self.temporal_sequence.append({
                'timestamp': timestamp,
                'timestamp_index': timestamp_index,
                'entity_type': entity_type,
                'entity_id': entity_id
            })
        
        return entity_id
    
    def get_chronological_entities(self) -> List[Dict[str, Any]]:
        """
        Get all temporal entities in chronological order with timestamp indices
        Compatible with graf frame generation
        """
        # Sort by timestamp, then by timestamp_index
        sorted_sequence = sorted(
            self.temporal_sequence,
            key=lambda x: (x['timestamp'], x['timestamp_index'])
        )
        
        entities = []
        for seq in sorted_sequence:
            entity_type = seq['entity_type']
            entity_id = seq['entity_id']
            
            # Find entity in store
            if entity_type in self.temporal_entities:
                for entity in self.temporal_entities[entity_type]:
                    if entity.get('entity_id') == entity_id:
                        entities.append(entity)
                        break
        
        return entities
    
    def export_for_graf(self) -> Dict[str, Any]:
        """
        Export in format compatible with graf
        Similar to grap v2 output structure
        """
        entities = []
        
        # Add core entities
        for entity_type, entity_dict in self.core_entities.items():
            for entity_id, entity_data in entity_dict.items():
                entity = {
                    'entity_id': entity_id,
                    'entity_type': entity_type.lower(),
                    **entity_data
                }
                entities.append(entity)
        
        # Add temporal entities in chronological order
        chronological = self.get_chronological_entities()
        entities.extend(chronological)
        
        # Re-sort everything by timestamp and index
        entities.sort(key=lambda x: (
            x.get('timestamp', ''),
            x.get('timestamp_index', 0)
        ))
        
        return {
            'grap_version': 'v3.0.0-alpha',
            'extraction_time': datetime.now().isoformat(),
            'total_entities': len(entities),
            'entities': entities
        }


class EnhancedSchemaEngine:
    """
    Complete schema-driven extraction engine with all missing features
    """
    
    def __init__(self, schema_dir: Path):
        self.schema_loader = SchemaLoader(schema_dir)
        self.extractor = EnhancedDataExtractor(self.schema_loader)
        self.entity_store = EnhancedEntityStore()
        
        # Multi-line pattern state
        self.multiline_buffer = []
        self.multiline_pattern = None
        
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Process a single log file
        """
        self.extractor.set_context(file_path=str(file_path))
        
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                self._process_line(line, line_num)
        
        return self.entity_store.export_for_graf()
    
    def process_files(self, file_paths: List[Path]) -> Dict[str, Any]:
        """
        Process multiple log files maintaining chronological order
        """
        all_extractions = []
        
        for file_path in file_paths:
            self.extractor.set_context(file_path=str(file_path))
            
            with open(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    entities = self._process_line(line, line_num)
                    all_extractions.extend(entities)
        
        # Sort all extractions chronologically
        all_extractions.sort(key=lambda x: (
            x.get('timestamp', ''),
            x.get('timestamp_index', 0)
        ))
        
        # Add to entity store in chronological order
        for entity_data in all_extractions:
            entity_type = entity_data.get('entity_type')
            if entity_type:
                category = self.schema_loader.get_entity_category(entity_type)
                if category == EntityCategory.CORE:
                    self.entity_store.add_core_entity(entity_type, entity_data)
                elif category == EntityCategory.TEMPORAL:
                    self.entity_store.add_temporal_entity(entity_type, entity_data)
        
        return self.entity_store.export_for_graf()
    
    def _process_line(self, line: str, line_num: int) -> List[Dict[str, Any]]:
        """
        Process a single line and return extracted entities
        """
        # Detect local node name if not set
        if not self.extractor.local_node_name:
            self._detect_local_node(line)
        
        # Extract entities from line
        extracted = self.extractor.extract_from_line(line, line_num)
        
        # Add to entity store
        for entity_data in extracted:
            entity_type = entity_data.get('entity_type')
            if not entity_type:
                continue
            
            category = self.schema_loader.get_entity_category(entity_type)
            if category == EntityCategory.CORE:
                self.entity_store.add_core_entity(entity_type, entity_data)
            elif category == EntityCategory.TEMPORAL:
                self.entity_store.add_temporal_entity(entity_type, entity_data)
        
        return extracted
    
    def _detect_local_node(self, line: str):
        """Detect local node name from log line"""
        # Try various patterns
        patterns = [
            r"wsrep_node_name\s*[:=]\s*'?([^']+)'?",
            r"Setting\s+wsrep_node_name\s+to\s+'([^']+)'",
            r"Node\s+name:\s*'?([^']+)'?",
            r"Server\s+([^\s]+)\s+synced\s+with\s+group",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                node_name = match.group(1).strip()
                if node_name and node_name.lower() not in {'unknown', 'joiner', 'donor'}:
                    self.extractor.set_context(node_name=node_name)
                    return
        
        # Try My UUID pattern
        match = re.search(r'My UUID:\s+([a-f0-9-]+)', line)
        if match:
            self.extractor.set_context(node_uuid=match.group(1))


def create_enhanced_engine(schema_dir: Path = None) -> EnhancedSchemaEngine:
    """Factory function to create enhanced engine"""
    if schema_dir is None:
        schema_dir = Path(__file__).parent.parent / 'schema'
    
    return EnhancedSchemaEngine(schema_dir)


if __name__ == '__main__':
    # Quick test
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python enhanced_schema_engine.py <log_file>")
        sys.exit(1)
    
    engine = create_enhanced_engine()
    result = engine.process_file(Path(sys.argv[1]))
    print(json.dumps(result, indent=2, default=str))
