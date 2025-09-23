"""
Output formatting for entity extraction results

This module provides various output formats for extracted entities,
including JSON, YAML, and text formats with compatibility for existing
grambo tools.
"""

import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from ..entities import Entity, EntityType
from ..entities.temporal_entities import TemporalStateTransferEntity
from ..entities.relationships import RelationshipManager


class OutputFormatter:
    """
    Formats entity extraction results in various output formats
    
    This class provides consistent formatting across different output
    types while maintaining compatibility with existing grambo tools.
    """
    
    def __init__(self, format_type: str = "text", show_stats: bool = False,
                 compact: bool = False, relationship_manager: Optional[RelationshipManager] = None):
        """
        Initialize output formatter
        
        Args:
            format_type: Output format ('text', 'json', 'yaml')
            show_stats: Include parsing statistics in output
            compact: Use compact formatting when possible
            relationship_manager: Optional relationship manager for including relationships
        """
        self.format_type = format_type.lower()
        self.show_stats = show_stats
        self.compact = compact
        self.relationship_manager = relationship_manager
        
        # Validate format type
        if self.format_type not in ['text', 'json', 'yaml']:
            raise ValueError(f"Unsupported format type: {format_type}")
            
    def format(self, entities: List[Entity], stats: Optional[Dict[str, Any]] = None) -> str:
        """
        Format entities according to the configured format type
        
        Args:
            entities: List of entities to format
            stats: Optional parsing statistics
            
        Returns:
            str: Formatted output
        """
        if self.format_type == 'json':
            return self._format_json(entities, stats)
        elif self.format_type == 'yaml':
            return self._format_yaml(entities, stats)
        else:  # text
            return self._format_text(entities, stats)
            
    def _format_json(self, entities: List[Entity], stats: Optional[Dict[str, Any]] = None) -> str:
        """Format entities as JSON"""
        def datetime_handler(obj):
            """JSON serializer for datetime objects"""
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
        
        output_data = {
            'metadata': {
                'generator': 'grap',
                'version': '2.0.0-alpha1',
                'timestamp': datetime.now().isoformat(),
                'total_entities': len(entities)
            },
            'entities': []
        }
        
        # Add entities
        for entity in entities:
            entity_dict = entity.to_dict()
            output_data['entities'].append(entity_dict)
            
        # Add statistics if requested
        if self.show_stats and stats:
            output_data['statistics'] = stats
            
        # Add relationships if relationship manager is available
        if self.relationship_manager:
            relationships = []
            for rel in self.relationship_manager.relationships.values():
                relationships.append(rel.to_dict())
            
            output_data['relationships'] = relationships
            output_data['metadata']['total_relationships'] = len(relationships)
            
            # Add relationship statistics
            rel_stats = self.relationship_manager.get_relationship_stats()
            output_data['metadata']['relationship_stats'] = rel_stats
        
        # Format JSON using custom datetime handler
        if self.compact:
            return json.dumps(output_data, separators=(',', ':'), default=datetime_handler)
        else:
            return json.dumps(output_data, indent=2, sort_keys=True, default=datetime_handler)
    
    def _convert_datetimes_to_iso(self, data):
        """Recursively convert datetime objects to ISO strings."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, datetime):
                    data[key] = value.isoformat()
                elif isinstance(value, (dict, list)):
                    self._convert_datetimes_to_iso(value)
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, datetime):
                    data[i] = item.isoformat()
                elif isinstance(item, (dict, list)):
                    self._convert_datetimes_to_iso(item)
            
    def _format_yaml(self, entities: List[Entity], stats: Optional[Dict[str, Any]] = None) -> str:
        """Format entities as YAML"""
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required for YAML output format")
            
        output_data = {
            'metadata': {
                'generator': 'grap',
                'version': '2.0.0-alpha1',
                'timestamp': datetime.now().isoformat(),
                'total_entities': len(entities)
            },
            'entities': []
        }
        
        # Add entities
        for entity in entities:
            entity_dict = entity.to_dict()
            # Convert datetime objects to ISO strings
            if entity_dict.get('timestamp'):
                if isinstance(entity_dict['timestamp'], datetime):
                    entity_dict['timestamp'] = entity_dict['timestamp'].isoformat()
            output_data['entities'].append(entity_dict)
            
        # Add statistics if requested
        if self.show_stats and stats:
            output_data['statistics'] = stats
            
        return yaml.dump(output_data, default_flow_style=False, sort_keys=True)
        
    def _format_text(self, entities: List[Entity], stats: Optional[Dict[str, Any]] = None) -> str:
        """Format entities as human-readable text"""
        lines = []
        
        # Header
        lines.append("GRAP Entity Extraction Results")
        lines.append("=" * 40)
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Total entities: {len(entities)}")
        lines.append("")
        
        # Group entities by type
        entities_by_type = self._group_entities_by_type(entities)
        
        for entity_type, entity_list in entities_by_type.items():
            lines.append(f"{entity_type} ({len(entity_list)} entities)")
            lines.append("-" * (len(entity_type) + 20))
            
            for entity in entity_list:
                lines.append(self._format_entity_text(entity))
                lines.append("")
                
        # Add statistics if requested
        if self.show_stats and stats:
            lines.append("Parsing Statistics")
            lines.append("-" * 20)
            for key, value in stats.items():
                if key != 'patterns_used':  # Skip complex data
                    lines.append(f"{key}: {value}")
            lines.append("")
            
        return "\n".join(lines)
        
    def _group_entities_by_type(self, entities: List[Entity]) -> Dict[str, List[Entity]]:
        """Group entities by their type"""
        grouped = {}
        for entity in entities:
            # Handle both enum and string entity types
            if hasattr(entity.entity_type, 'value'):
                entity_type = entity.entity_type.value
            else:
                entity_type = str(entity.entity_type)
                
            if entity_type not in grouped:
                grouped[entity_type] = []
            grouped[entity_type].append(entity)
            
        # Sort entities within each group by timestamp or line number
        for entity_list in grouped.values():
            entity_list.sort(key=lambda e: (e.timestamp or datetime.min, e.line_number or 0))
            
        return grouped
        
    def _format_entity_text(self, entity: Entity) -> str:
        """Format a single entity as text"""
        lines = []
        
        # Basic info with entity ID
        timestamp_str = ""
        if entity.timestamp:
            if hasattr(entity.timestamp, 'strftime'):
                timestamp_str = entity.timestamp.strftime('%H:%M:%S')
            else:
                timestamp_str = str(entity.timestamp)
        elif entity.line_number:
            timestamp_str = f"Line {entity.line_number}"
            
        confidence_str = f"({entity.confidence:.2f})" if entity.confidence < 1.0 else ""
        entity_id_short = entity.entity_id[:8] if hasattr(entity, 'entity_id') and entity.entity_id else "unknown"
        
        lines.append(f"  {timestamp_str} {confidence_str} [ID: {entity_id_short}]")
        
        # Entity-specific information
        if isinstance(entity, TemporalStateTransferEntity):
            lines.append(self._format_temporal_state_transfer_text(entity))
        elif entity.entity_type == EntityType.NODE:
            lines.append(self._format_node_entity_text(entity))
        elif entity.entity_type == EntityType.STATE_TRANSFER:
            lines.append(self._format_state_transfer_entity_text(entity))
        elif entity.entity_type == EntityType.VIEW:
            lines.append(self._format_view_entity_text(entity))
        else:
            # Generic entity formatting
            entity_dict = entity.to_dict()
            key_fields = ['event_name', 'description', 'message']
            for field in key_fields:
                if field in entity_dict and entity_dict[field]:
                    lines.append(f"    {field}: {entity_dict[field]}")
                    break
                    
        return "\n".join(lines)
        
    def _format_node_entity_text(self, entity) -> str:
        """Format node entity as text"""
        parts = []
        
        if hasattr(entity, 'node_id') and entity.node_id:
            parts.append(f"Node: {entity.node_id[:8]}...")
        elif hasattr(entity, 'node_name') and entity.node_name:
            parts.append(f"Node: {entity.node_name}")
            
        if hasattr(entity, 'current_state') and entity.current_state:
            state_str = entity.current_state.value if hasattr(entity.current_state, 'value') else str(entity.current_state)
            if hasattr(entity, 'previous_state') and entity.previous_state:
                prev_state_str = entity.previous_state.value if hasattr(entity.previous_state, 'value') else str(entity.previous_state)
                parts.append(f"State: {prev_state_str} -> {state_str}")
            else:
                parts.append(f"State: {state_str}")
                
        if hasattr(entity, 'node_address') and entity.node_address:
            parts.append(f"Address: {entity.node_address}")
            
        return "    " + " | ".join(parts)
    
    def _format_temporal_state_transfer_text(self, entity: TemporalStateTransferEntity) -> str:
        """Format temporal state transfer entity as text with clean timeline"""
        lines = []
        
        # Basic SST info line
        parts = []
        if entity.transfer_type:
            transfer_type = entity.transfer_type.value if hasattr(entity.transfer_type, 'value') else str(entity.transfer_type)
            parts.append(f"Type: {transfer_type}")
        
        if entity.transfer_method:
            method = entity.transfer_method.value if hasattr(entity.transfer_method, 'value') else str(entity.transfer_method)
            parts.append(f"Method: {method}")
            
        # Show donor->joiner flow
        nodes = []
        if entity.donor_node and entity.donor_node != 'unknown_donor':
            nodes.append(f"Donor: {entity.donor_node}")
        if entity.joiner_node and entity.joiner_node != 'unknown_joiner':
            nodes.append(f"Joiner: {entity.joiner_node}")
        if nodes:
            parts.append(" | ".join(nodes))
            
        # Current status and duration
        parts.append(f"Status: {entity.current_status}")
        
        if entity.duration and entity.duration > 0:
            if entity.duration < 60:
                duration_str = f"{entity.duration:.1f}s"
            elif entity.duration < 3600:
                duration_str = f"{entity.duration/60:.1f}m"
            else:
                duration_str = f"{entity.duration/3600:.1f}h"
            parts.append(f"Duration: {duration_str}")
            
        lines.append("    " + " | ".join(parts))
        
        # Timeline summary (only show meaningful changes)
        timeline = entity.get_timeline()
        if timeline and 'transfer_status' in timeline:
            status_changes = timeline['transfer_status']
            if len(status_changes) > 1:  # Only show if there were status changes
                lines.append("    Timeline:")
                
                for i, (timestamp, status) in enumerate(status_changes):
                    timestamp_str = timestamp.strftime('%H:%M:%S') if hasattr(timestamp, 'strftime') else str(timestamp)
                    if i == 0:
                        lines.append(f"      {timestamp_str}: Started ({status})")
                    else:
                        lines.append(f"      {timestamp_str}: {status}")
                        
        # Show transfer statistics if available
        stats_shown = False
        if hasattr(entity, 'transferred_bytes') and entity.transferred_bytes and entity.transferred_bytes > 0:
            size_str = self._format_bytes(entity.transferred_bytes)
            lines.append(f"    Transferred: {size_str}")
            stats_shown = True
            
        if hasattr(entity, 'transfer_rate') and entity.transfer_rate and entity.transfer_rate > 0:
            rate_str = self._format_bytes(entity.transfer_rate) + "/s"
            lines.append(f"    Final rate: {rate_str}")
            stats_shown = True
            
        return "\n".join(lines)
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes in human-readable format"""
        if bytes_val >= 1024*1024*1024:  # GB
            return f"{bytes_val/(1024*1024*1024):.1f} GB"
        elif bytes_val >= 1024*1024:  # MB
            return f"{bytes_val/(1024*1024):.1f} MB"
        else:  # KB or bytes
            return f"{bytes_val/1024:.1f} KB"
        
    def _format_state_transfer_entity_text(self, entity) -> str:
        """Format state transfer entity as text"""
        parts = []
        
        if hasattr(entity, 'transfer_type') and entity.transfer_type:
            transfer_type = entity.transfer_type.value if hasattr(entity.transfer_type, 'value') else str(entity.transfer_type)
            parts.append(f"Type: {transfer_type}")
            
        if hasattr(entity, 'transfer_method') and entity.transfer_method:
            method = entity.transfer_method.value if hasattr(entity.transfer_method, 'value') else str(entity.transfer_method)
            parts.append(f"Method: {method}")
            
        if hasattr(entity, 'transfer_status') and entity.transfer_status:
            parts.append(f"Status: {entity.transfer_status}")
            
        if hasattr(entity, 'donor_node') and entity.donor_node:
            parts.append(f"Donor: {entity.donor_node[:8]}...")
        if hasattr(entity, 'joiner_node') and entity.joiner_node:
            parts.append(f"Joiner: {entity.joiner_node[:8]}...")
            
        if hasattr(entity, 'seqno_start') and hasattr(entity, 'seqno_end'):
            if entity.seqno_start is not None and entity.seqno_end is not None:
                parts.append(f"Seqno: {entity.seqno_start}-{entity.seqno_end}")
                
        return "    " + " | ".join(parts)
        
    def _format_view_entity_text(self, entity) -> str:
        """Format view entity as text"""
        parts = []
        
        if hasattr(entity, 'view_id') and entity.view_id:
            parts.append(f"View: {entity.view_id[:8]}...")
        if hasattr(entity, 'view_seq') and entity.view_seq is not None:
            parts.append(f"Seq: {entity.view_seq}")
            
        if hasattr(entity, 'cluster_state') and entity.cluster_state:
            parts.append(f"State: {entity.cluster_state}")
            
        if hasattr(entity, 'members') and entity.members:
            parts.append(f"Members: {len(entity.members)}")
            
        if hasattr(entity, 'joined_nodes') and entity.joined_nodes:
            parts.append(f"Joined: {len(entity.joined_nodes)}")
        if hasattr(entity, 'left_nodes') and entity.left_nodes:
            parts.append(f"Left: {len(entity.left_nodes)}")
            
        return "    " + " | ".join(parts)
        
    def format_compatible(self, entities: List[Entity]) -> Dict[str, Any]:
        """
        Format entities in a format compatible with existing grambo tools
        
        This method provides backward compatibility with the existing
        grambo/gras output format for integration with grambo-web.
        
        Args:
            entities: List of entities to format
            
        Returns:
            Dict[str, Any]: Compatible format data structure
        """
        # Group entities by type for processing
        entities_by_type = self._group_entities_by_type(entities)
        
        # Build compatible structure
        compatible_data = {
            'metadata': {
                'generator': 'grap',
                'version': '2.0.0-alpha1',
                'timestamp': datetime.now().isoformat(),
                'entity_count': len(entities),
                'processing_method': 'entity_extraction'
            },
            'detailed_events': [],
            'cluster_events': [],
            'summary': {
                'node_count': 0,
                'state_transfers': 0,
                'view_changes': 0,
                'timeline_start': None,
                'timeline_end': None
            }
        }
        
        # Convert entities to compatible format
        for entity in entities:
            # Add to detailed_events (compatible with existing format)
            detailed_event = self._entity_to_detailed_event(entity)
            compatible_data['detailed_events'].append(detailed_event)
            
            # Add to cluster_events if it's a significant event
            if self._is_cluster_event(entity):
                cluster_event = self._entity_to_cluster_event(entity)
                compatible_data['cluster_events'].append(cluster_event)
                
        # Update summary
        compatible_data['summary'].update(self._calculate_summary(entities))
        
        return compatible_data
        
    def _entity_to_detailed_event(self, entity: Entity) -> Dict[str, Any]:
        """Convert entity to detailed_event format"""
        # Handle entity_type safely - could be enum or string
        if hasattr(entity.entity_type, 'value'):
            event_type = entity.entity_type.value
        else:
            event_type = str(entity.entity_type)
            
        event = {
            'timestamp': entity.timestamp.isoformat() if entity.timestamp else None,
            'line_number': entity.line_number,
            'event_type': event_type,
            'raw_line': entity.raw_line,
            'confidence': entity.confidence,
            'pattern': entity.pattern_name,
            'details': {}
        }
        
        # Add entity-specific details
        entity_dict = entity.to_dict()
        for key, value in entity_dict.items():
            if key not in ['entity_id', 'entity_type', 'timestamp', 'line_number', 
                          'raw_line', 'confidence', 'pattern_name', 'extraction_method']:
                if value is not None:
                    event['details'][key] = value
                    
        return event
        
    def _entity_to_cluster_event(self, entity: Entity) -> Dict[str, Any]:
        """Convert entity to cluster_event format for significant events"""
        # Handle entity_type safely - could be enum or string
        if hasattr(entity.entity_type, 'value'):
            event_type = entity.entity_type.value
        else:
            event_type = str(entity.entity_type)
            
        event = {
            'timestamp': entity.timestamp.isoformat() if entity.timestamp else None,
            'event_type': event_type,
            'description': self._generate_event_description(entity),
            'nodes_involved': self._extract_nodes_involved(entity),
            'significance': self._calculate_event_significance(entity)
        }
        
        return event
        
    def _is_cluster_event(self, entity: Entity) -> bool:
        """Determine if entity represents a significant cluster event"""
        # State transitions, view changes, and state transfers are cluster events
        if entity.entity_type in [EntityType.VIEW, EntityType.STATE_TRANSFER]:
            return True
            
        # Node state changes are also cluster events
        if entity.entity_type == EntityType.NODE:
            if hasattr(entity, 'current_state') and hasattr(entity, 'previous_state'):
                return entity.previous_state is not None
                
        return False
        
    def _generate_event_description(self, entity: Entity) -> str:
        """Generate human-readable description for entity"""
        if entity.entity_type == EntityType.NODE:
            if hasattr(entity, 'current_state') and hasattr(entity, 'previous_state'):
                if entity.previous_state:
                    # Handle enum values safely
                    prev_state_str = entity.previous_state.value if hasattr(entity.previous_state, 'value') else str(entity.previous_state)
                    current_state_str = entity.current_state.value if hasattr(entity.current_state, 'value') else str(entity.current_state)
                    return f"Node state change: {prev_state_str} -> {current_state_str}"
                else:
                    current_state_str = entity.current_state.value if hasattr(entity.current_state, 'value') else str(entity.current_state)
                    return f"Node state: {current_state_str}"
        elif entity.entity_type == EntityType.STATE_TRANSFER:
            if hasattr(entity, 'transfer_type') and hasattr(entity, 'transfer_status'):
                transfer_type_str = entity.transfer_type.value if hasattr(entity.transfer_type, 'value') else str(entity.transfer_type)
                return f"{transfer_type_str} {entity.transfer_status}"
        elif entity.entity_type == EntityType.VIEW:
            if hasattr(entity, 'cluster_state'):
                return f"Cluster view change: {entity.cluster_state}"
                
        # Handle entity_type safely for the final return
        if hasattr(entity.entity_type, 'value'):
            entity_type_str = entity.entity_type.value
        else:
            entity_type_str = str(entity.entity_type)
            
        return f"{entity_type_str} event"
        
    def _extract_nodes_involved(self, entity: Entity) -> List[str]:
        """Extract list of nodes involved in the entity/event"""
        nodes = []
        
        # Check common node fields
        for field in ['node_id', 'donor_node', 'joiner_node']:
            if hasattr(entity, field):
                value = getattr(entity, field)
                if value and value not in nodes:
                    nodes.append(value)
                    
        # For view entities, check members
        if hasattr(entity, 'members'):
            nodes.extend(entity.members)
            
        return nodes
        
    def _calculate_event_significance(self, entity: Entity) -> float:
        """Calculate significance score for the event"""
        # Base significance on entity type and confidence
        base_significance = {
            EntityType.VIEW: 0.9,
            EntityType.STATE_TRANSFER: 0.8,
            EntityType.NODE: 0.7,
        }.get(entity.entity_type, 0.5)
        
        return min(1.0, base_significance * entity.confidence)
        
    def _calculate_summary(self, entities: List[Entity]) -> Dict[str, Any]:
        """Calculate summary statistics from entities"""
        summary = {
            'node_count': 0,
            'state_transfers': 0,
            'view_changes': 0,
            'timeline_start': None,
            'timeline_end': None
        }
        
        node_ids = set()
        timestamps = []
        
        for entity in entities:
            # Collect timestamps
            if entity.timestamp:
                timestamps.append(entity.timestamp)
                
            # Count by type
            if entity.entity_type == EntityType.NODE:
                if hasattr(entity, 'node_id') and entity.node_id:
                    node_ids.add(entity.node_id)
            elif entity.entity_type == EntityType.STATE_TRANSFER:
                summary['state_transfers'] += 1
            elif entity.entity_type == EntityType.VIEW:
                summary['view_changes'] += 1
                
        summary['node_count'] = len(node_ids)
        
        if timestamps:
            summary['timeline_start'] = min(timestamps).isoformat()
            summary['timeline_end'] = max(timestamps).isoformat()
            
        return summary