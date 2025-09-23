#!/usr/bin/env python3
"""
Entity Relationship System for GRAP

This module implements the comprehensive relationship system that connects
related entities extracted from Galera cluster logs, enabling contextual
analysis and causality tracking.
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Set, Any, Optional, Tuple
from collections import defaultdict

from .base import Entity, EntityType


class RelationshipType(Enum):
    """Types of relationships between entities"""
    
    # Node-View Relationships
    TRIGGERS = "triggers"           # Entity causes another entity
    INCLUDES = "includes"           # View includes node as active member
    ADDS = "adds"                  # View adds node to membership
    REMOVES = "removes"            # View removes node from membership  
    PARTITIONS = "partitions"      # View marks node as partitioned
    SUPERSEDES = "supersedes"      # Sequential replacement relationship
    
    # SST Relationships
    REQUESTS = "requests"          # Node requests state transfer
    PROVIDES = "provides"          # Node provides state transfer
    INVOLVES_DONOR = "involves_donor"    # SST involves specific donor
    INVOLVES_JOINER = "involves_joiner"  # SST involves specific joiner
    FOLLOWS = "follows"            # Sequential events in same session
    
    # Error/Warning Relationships
    EXPERIENCES = "experiences"    # Node experiences error/warning
    AFFECTS = "affects"           # Error/warning affects specific entity
    RELATES_TO = "relates_to"     # Contextual relationship
    CAUSES = "causes"             # Causal relationship
    
    # Cluster Relationships
    PEER = "peer"                 # Equal relationship between same types


@dataclass
class EntityRelationship:
    """Defines a relationship between two entities"""
    source_entity_id: str
    target_entity_id: str
    relationship_type: RelationshipType
    confidence: float
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    relationship_id: str = ""
    
    def __post_init__(self):
        if not self.relationship_id:
            self.relationship_id = self._generate_relationship_id()
        
        # Validate confidence
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")
    
    def _generate_relationship_id(self) -> str:
        """Generate unique relationship ID"""
        hash_input = f"{self.source_entity_id}_{self.target_entity_id}_{self.relationship_type.value}"
        return f"rel_{hashlib.md5(hash_input.encode()).hexdigest()[:8]}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'relationship_id': self.relationship_id,
            'source_entity_id': self.source_entity_id,
            'target_entity_id': self.target_entity_id,
            'relationship_type': self.relationship_type.value,
            'confidence': self.confidence,
            'created_at': self.created_at.isoformat(),
            'metadata': self.metadata
        }


class RelationshipManager:
    """Manages entity relationships with efficient indexing and querying"""
    
    def __init__(self):
        self.relationships: Dict[str, EntityRelationship] = {}
        self.source_index: Dict[str, Set[str]] = defaultdict(set)  # source_id -> relationship_ids
        self.target_index: Dict[str, Set[str]] = defaultdict(set)  # target_id -> relationship_ids
        self.type_index: Dict[RelationshipType, Set[str]] = defaultdict(set)  # type -> relationship_ids
        self.logger = logging.getLogger(__name__)
    
    def add_relationship(self, relationship: EntityRelationship) -> bool:
        """Add relationship with indexing"""
        try:
            rel_id = relationship.relationship_id
            
            # Check for duplicate
            if rel_id in self.relationships:
                self.logger.debug(f"Relationship {rel_id} already exists, skipping")
                return False
            
            # Store relationship
            self.relationships[rel_id] = relationship
            
            # Update indexes
            self.source_index[relationship.source_entity_id].add(rel_id)
            self.target_index[relationship.target_entity_id].add(rel_id)
            self.type_index[relationship.relationship_type].add(rel_id)
            
            self.logger.debug(f"Added relationship: {relationship.source_entity_id} --{relationship.relationship_type.value}--> {relationship.target_entity_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error adding relationship: {e}")
            return False
    
    def get_related_entities(self, entity_id: str, 
                           relationship_types: Optional[List[RelationshipType]] = None,
                           direction: str = "both") -> List[EntityRelationship]:
        """
        Find all related entities
        
        Args:
            entity_id: The entity to find relationships for
            relationship_types: Filter by specific relationship types
            direction: "outgoing", "incoming", or "both"
        """
        result_relationships = []
        
        # Get relationship IDs based on direction
        if direction in ["outgoing", "both"]:
            outgoing_rel_ids = self.source_index.get(entity_id, set())
            result_relationships.extend([self.relationships[rel_id] for rel_id in outgoing_rel_ids])
        
        if direction in ["incoming", "both"]:
            incoming_rel_ids = self.target_index.get(entity_id, set())
            result_relationships.extend([self.relationships[rel_id] for rel_id in incoming_rel_ids])
        
        # Filter by relationship types if specified
        if relationship_types:
            result_relationships = [
                rel for rel in result_relationships 
                if rel.relationship_type in relationship_types
            ]
        
        return result_relationships
    
    def get_relationship_chain(self, start_entity_id: str, 
                             end_entity_id: Optional[str] = None,
                             max_hops: int = 5,
                             relationship_types: Optional[List[RelationshipType]] = None) -> List[EntityRelationship]:
        """
        Find relationship path between entities using breadth-first search
        
        Args:
            start_entity_id: Starting entity
            end_entity_id: Target entity (if None, returns all reachable entities)
            max_hops: Maximum relationship hops to traverse
            relationship_types: Filter path by specific relationship types
        """
        if max_hops <= 0:
            return []
        
        visited = set()
        queue = [(start_entity_id, [])]  # (entity_id, path)
        
        while queue:
            current_entity, path = queue.pop(0)
            
            if current_entity in visited:
                continue
            
            visited.add(current_entity)
            
            # If we found the target, return the path
            if end_entity_id and current_entity == end_entity_id and path:
                return path
            
            # If we've reached max hops, skip this branch
            if len(path) >= max_hops:
                continue
            
            # Get outgoing relationships
            outgoing_rels = self.get_related_entities(
                current_entity, 
                relationship_types=relationship_types,
                direction="outgoing"
            )
            
            for rel in outgoing_rels:
                if rel.target_entity_id not in visited:
                    new_path = path + [rel]
                    queue.append((rel.target_entity_id, new_path))
        
        # If no specific end entity, return all found relationships
        if not end_entity_id:
            all_relationships = []
            for entity_id in visited:
                if entity_id != start_entity_id:
                    all_relationships.extend(
                        self.get_related_entities(entity_id, relationship_types, "both")
                    )
            return list(set(all_relationships))  # Remove duplicates
        
        return []  # No path found
    
    def get_entities_by_relationship_type(self, relationship_type: RelationshipType) -> List[EntityRelationship]:
        """Get all relationships of a specific type"""
        rel_ids = self.type_index.get(relationship_type, set())
        return [self.relationships[rel_id] for rel_id in rel_ids]
    
    def get_relationship_stats(self) -> Dict[str, Any]:
        """Get statistics about stored relationships"""
        stats = {
            'total_relationships': len(self.relationships),
            'by_type': {},
            'unique_entities': set(),
            'most_connected_entities': []
        }
        
        # Count by type
        for rel_type in RelationshipType:
            count = len(self.type_index.get(rel_type, set()))
            if count > 0:
                stats['by_type'][rel_type.value] = count
        
        # Count unique entities
        for rel in self.relationships.values():
            stats['unique_entities'].add(rel.source_entity_id)
            stats['unique_entities'].add(rel.target_entity_id)
        
        stats['unique_entities'] = len(stats['unique_entities'])
        
        # Find most connected entities
        entity_connections = defaultdict(int)
        for entity_id in self.source_index:
            entity_connections[entity_id] += len(self.source_index[entity_id])
        for entity_id in self.target_index:
            entity_connections[entity_id] += len(self.target_index[entity_id])
        
        stats['most_connected_entities'] = sorted(
            entity_connections.items(), 
            key=lambda x: x[1], 
            reverse=True
        )[:10]
        
        return stats
    
    def clear(self):
        """Clear all relationships and indexes"""
        self.relationships.clear()
        self.source_index.clear()
        self.target_index.clear()
        self.type_index.clear()
        self.logger.info("Cleared all relationships")


class RelationshipDiscovery:
    """Discovers relationships between entities using various correlation methods"""
    
    def __init__(self, relationship_manager: RelationshipManager):
        self.relationship_manager = relationship_manager
        self.logger = logging.getLogger(__name__)
    
    def discover_all_relationships(self, entities: List[Entity]) -> int:
        """
        Discover all relationships between the given entities
        
        Returns:
            Number of relationships discovered
        """
        relationships_found = 0
        
        # Sort entities by timestamp for temporal analysis
        def safe_timestamp(entity):
            """Get entity timestamp safely, handling both datetime and string types"""
            if entity.timestamp is None:
                return datetime.min
            elif isinstance(entity.timestamp, datetime):
                return entity.timestamp
            elif isinstance(entity.timestamp, str):
                try:
                    # Try to parse string timestamp
                    return datetime.fromisoformat(entity.timestamp.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    # If parsing fails, return minimum datetime
                    return datetime.min
            else:
                # For any other type, return minimum datetime
                return datetime.min
        
        sorted_entities = sorted(entities, key=safe_timestamp)
        
        # Discover different types of relationships
        relationships_found += self._discover_view_node_relationships(sorted_entities)
        relationships_found += self._discover_sst_relationships(sorted_entities)
        relationships_found += self._discover_error_relationships(sorted_entities)
        relationships_found += self._discover_temporal_relationships(sorted_entities)
        
        self.logger.info(f"Discovered {relationships_found} relationships")
        return relationships_found
    
    def _discover_view_node_relationships(self, entities: List[Entity]) -> int:
        """Discover relationships between VIEW and NODE entities"""
        relationships_found = 0
        
        view_entities = [e for e in entities if e.entity_type == EntityType.VIEW]
        node_entities = [e for e in entities if e.entity_type == EntityType.NODE]
        
        for view_entity in view_entities:
            # Create VIEW -> NODE relationships based on membership
            members = getattr(view_entity, 'members', [])
            joined_nodes = getattr(view_entity, 'joined_nodes', [])
            left_nodes = getattr(view_entity, 'left_nodes', [])
            partitioned_nodes = getattr(view_entity, 'partitioned_nodes', [])
            
            # INCLUDES relationships for current members
            for member in members:
                matching_nodes = self._find_nodes_by_identifier(node_entities, member)
                for node in matching_nodes:
                    rel = EntityRelationship(
                        source_entity_id=view_entity.entity_id,
                        target_entity_id=node.entity_id,
                        relationship_type=RelationshipType.INCLUDES,
                        confidence=0.95,
                        metadata={'member_id': member}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
            
            # ADDS relationships for joined nodes
            for joined in joined_nodes:
                matching_nodes = self._find_nodes_by_identifier(node_entities, joined)
                for node in matching_nodes:
                    rel = EntityRelationship(
                        source_entity_id=view_entity.entity_id,
                        target_entity_id=node.entity_id,
                        relationship_type=RelationshipType.ADDS,
                        confidence=0.95,
                        metadata={'joined_id': joined}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
            
            # REMOVES relationships for left nodes
            for left in left_nodes:
                matching_nodes = self._find_nodes_by_identifier(node_entities, left)
                for node in matching_nodes:
                    rel = EntityRelationship(
                        source_entity_id=view_entity.entity_id,
                        target_entity_id=node.entity_id,
                        relationship_type=RelationshipType.REMOVES,
                        confidence=0.95,
                        metadata={'left_id': left}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
            
            # PARTITIONS relationships for partitioned nodes
            for partitioned in partitioned_nodes:
                matching_nodes = self._find_nodes_by_identifier(node_entities, partitioned)
                for node in matching_nodes:
                    rel = EntityRelationship(
                        source_entity_id=view_entity.entity_id,
                        target_entity_id=node.entity_id,
                        relationship_type=RelationshipType.PARTITIONS,
                        confidence=0.95,
                        metadata={'partitioned_id': partitioned}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
        
        # Create VIEW -> VIEW SUPERSEDES relationships
        view_entities.sort(key=lambda v: getattr(v, 'view_seq', 0))
        for i in range(1, len(view_entities)):
            current_view = view_entities[i]
            previous_view = view_entities[i-1]
            
            rel = EntityRelationship(
                source_entity_id=current_view.entity_id,
                target_entity_id=previous_view.entity_id,
                relationship_type=RelationshipType.SUPERSEDES,
                confidence=0.90,
                metadata={
                    'current_seq': getattr(current_view, 'view_seq', 0),
                    'previous_seq': getattr(previous_view, 'view_seq', 0)
                }
            )
            if self.relationship_manager.add_relationship(rel):
                relationships_found += 1
        
        return relationships_found
    
    def _discover_sst_relationships(self, entities: List[Entity]) -> int:
        """Discover relationships involving STATE_TRANSFER entities"""
        relationships_found = 0
        
        sst_entities = [e for e in entities if e.entity_type == EntityType.STATE_TRANSFER]
        node_entities = [e for e in entities if e.entity_type == EntityType.NODE]
        
        for sst_entity in sst_entities:
            # SST -> NODE relationships (INVOLVES_DONOR, INVOLVES_JOINER)
            donor_node = getattr(sst_entity, 'donor_node', '')
            joiner_node = getattr(sst_entity, 'joiner_node', '')
            
            if donor_node:
                matching_donors = self._find_nodes_by_identifier(node_entities, donor_node)
                for donor in matching_donors:
                    rel = EntityRelationship(
                        source_entity_id=sst_entity.entity_id,
                        target_entity_id=donor.entity_id,
                        relationship_type=RelationshipType.INVOLVES_DONOR,
                        confidence=0.95,
                        metadata={'donor_name': donor_node}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
            
            if joiner_node:
                matching_joiners = self._find_nodes_by_identifier(node_entities, joiner_node)
                for joiner in matching_joiners:
                    rel = EntityRelationship(
                        source_entity_id=sst_entity.entity_id,
                        target_entity_id=joiner.entity_id,
                        relationship_type=RelationshipType.INVOLVES_JOINER,
                        confidence=0.95,
                        metadata={'joiner_name': joiner_node}
                    )
                    if self.relationship_manager.add_relationship(rel):
                        relationships_found += 1
        
        # Group SST events into sessions and create FOLLOWS relationships
        sst_sessions = self._group_sst_sessions(sst_entities)
        for session_events in sst_sessions.values():
            session_events.sort(key=lambda e: e.timestamp or datetime.min)
            
            for i in range(1, len(session_events)):
                current_event = session_events[i]
                previous_event = session_events[i-1]
                
                rel = EntityRelationship(
                    source_entity_id=current_event.entity_id,
                    target_entity_id=previous_event.entity_id,
                    relationship_type=RelationshipType.FOLLOWS,
                    confidence=0.85,
                    metadata={'session_sequence': i}
                )
                if self.relationship_manager.add_relationship(rel):
                    relationships_found += 1
        
        return relationships_found
    
    def _discover_error_relationships(self, entities: List[Entity]) -> int:
        """Discover relationships involving ERROR and WARNING entities"""
        relationships_found = 0
        
        def safe_timestamp_extract(entity):
            """Extract timestamp safely, handling both datetime and string types"""
            if entity.timestamp is None:
                return None
            elif isinstance(entity.timestamp, datetime):
                return entity.timestamp
            elif isinstance(entity.timestamp, str):
                try:
                    # Try to parse string timestamp
                    return datetime.fromisoformat(entity.timestamp.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    return None
            else:
                return None
        
        error_entities = [e for e in entities if e.entity_type in [EntityType.ERROR, EntityType.WARNING]]
        node_entities = [e for e in entities if e.entity_type == EntityType.NODE]
        sst_entities = [e for e in entities if e.entity_type == EntityType.STATE_TRANSFER]
        
        for error_entity in error_entities:
            # Try to correlate error with nodes
            # This could be based on node names in error messages or temporal proximity
            error_timestamp = safe_timestamp_extract(error_entity)
            
            if error_timestamp:
                # Find nodes that had activity around the same time
                time_window = 10  # seconds
                for node_entity in node_entities:
                    node_timestamp = safe_timestamp_extract(node_entity)
                    if node_timestamp:
                        time_diff = abs((error_timestamp - node_timestamp).total_seconds())
                        if time_diff <= time_window:
                            confidence = max(0.5, 1.0 - (time_diff / time_window) * 0.3)
                            
                            rel = EntityRelationship(
                                source_entity_id=error_entity.entity_id,
                                target_entity_id=node_entity.entity_id,
                                relationship_type=RelationshipType.AFFECTS,
                                confidence=confidence,
                                metadata={'time_diff_seconds': time_diff}
                            )
                            if self.relationship_manager.add_relationship(rel):
                                relationships_found += 1
                
                # Find SST operations around the same time
                for sst_entity in sst_entities:
                    sst_timestamp = safe_timestamp_extract(sst_entity)
                    if sst_timestamp:
                        time_diff = abs((error_timestamp - sst_timestamp).total_seconds())
                        if time_diff <= time_window:
                            confidence = max(0.6, 1.0 - (time_diff / time_window) * 0.2)
                            
                            rel = EntityRelationship(
                                source_entity_id=error_entity.entity_id,
                                target_entity_id=sst_entity.entity_id,
                                relationship_type=RelationshipType.RELATES_TO,
                                confidence=confidence,
                                metadata={'time_diff_seconds': time_diff}
                            )
                            if self.relationship_manager.add_relationship(rel):
                                relationships_found += 1
        
        return relationships_found
    
    def _discover_temporal_relationships(self, entities: List[Entity]) -> int:
        """Discover temporal relationships based on timing patterns"""
        relationships_found = 0
        
        # This method can be expanded to discover more complex temporal patterns
        # For now, it's a placeholder for future temporal analysis
        
        return relationships_found
    
    def _find_nodes_by_identifier(self, node_entities: List[Entity], identifier: str) -> List[Entity]:
        """Find node entities that match the given identifier (name, ID, or UUID)"""
        matching_nodes = []
        
        for node in node_entities:
            # Check node_name
            if hasattr(node, 'node_name') and getattr(node, 'node_name') == identifier:
                matching_nodes.append(node)
                continue
            
            # Check node_id  
            if hasattr(node, 'node_id') and getattr(node, 'node_id') == identifier:
                matching_nodes.append(node)
                continue
            
            # Check long_uuid
            if hasattr(node, 'long_uuid') and getattr(node, 'long_uuid') == identifier:
                matching_nodes.append(node)
                continue
        
        return matching_nodes
    
    def _group_sst_sessions(self, sst_entities: List[Entity]) -> Dict[str, List[Entity]]:
        """Group SST entities into sessions based on donor/joiner pairs"""
        sessions = defaultdict(list)
        
        for sst_entity in sst_entities:
            donor = getattr(sst_entity, 'donor_node', 'unknown_donor')
            joiner = getattr(sst_entity, 'joiner_node', 'unknown_joiner')
            session_key = f"{donor}_{joiner}"
            sessions[session_key].append(sst_entity)
        
        return sessions