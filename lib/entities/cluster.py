"""
Cluster Entity Implementation - Root hierarchical container

This module implements the ClusterEntity as the root container for all other entities,
providing comprehensive cluster analysis with split-brain detection and multi-node 
log correlation capabilities.

Based on the comprehensive entity model design in ref/entities/CLUSTER_ENTITY.md
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, Counter
import logging

from .base import Entity, EntityType
from .core import (
    NodeEntity, StateTransferEntity, ViewEntity, CommunicationEntity,
    WarningEntity, ErrorEntity, PerformanceEntity, TransactionEntity
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ViewCollection:
    """
    Collection of views with perspective awareness for split-brain detection
    
    This class manages views from multiple nodes and can detect conflicting
    perspectives that indicate network partitions.
    """
    
    views_by_node: Dict[str, List[ViewEntity]] = field(default_factory=dict)
    views_by_id: Dict[str, List[ViewEntity]] = field(default_factory=dict)
    timeline: List[ViewEntity] = field(default_factory=list)
    
    def add_view(self, view: ViewEntity) -> "ViewCollection":
        """Return a new ViewCollection with the added view (immutable)"""
        from copy import deepcopy
        node_id = view.log_source or "unknown"
        views_by_node = deepcopy(self.views_by_node)
        views_by_id = deepcopy(self.views_by_id)
        timeline = list(self.timeline)
        # Add to node-specific collection
        if node_id not in views_by_node:
            views_by_node[node_id] = []
        views_by_node[node_id].append(view)
        # Add to view ID collection
        if view.view_id:
            if view.view_id not in views_by_id:
                views_by_id[view.view_id] = []
            views_by_id[view.view_id].append(view)
        # Add to timeline (sorted by timestamp)
        timeline.append(view)
        timeline.sort(key=lambda v: v.timestamp or datetime.min)
        return ViewCollection(
            views_by_node=views_by_node,
            views_by_id=views_by_id,
            timeline=timeline
        )
    
    def detect_split_brain(self) -> Dict[str, Any]:
        """
        Detect potential split-brain scenarios by analyzing conflicting views
        
        Returns:
            Dict with split-brain analysis results
        """
        split_brain_events = []
        
        # Group views by timestamp (within tolerance)
        timestamp_groups = defaultdict(list)
        tolerance_seconds = 30  # Views within 30 seconds are considered concurrent
        
        for view in self.timeline:
            if view.timestamp:
                # Round to nearest 30-second interval for grouping
                rounded_time = datetime.fromtimestamp(
                    (view.timestamp.timestamp() // tolerance_seconds) * tolerance_seconds
                )
                timestamp_groups[rounded_time].append(view)
        
        # Analyze each time group for conflicts
        for timestamp, views_at_time in timestamp_groups.items():
            if len(views_at_time) > 1:
                # Check for conflicting membership or states
                memberships = set()
                states = set()
                
                for view in views_at_time:
                    if view.members:
                        memberships.add(tuple(sorted(view.members)))
                    if view.cluster_state:
                        states.add(view.cluster_state)
                
                # Multiple different memberships at same time = split brain
                if len(memberships) > 1 or len(states) > 1:
                    split_brain_events.append({
                        'timestamp': timestamp,
                        'conflicting_views': len(views_at_time),
                        'different_memberships': len(memberships),
                        'different_states': len(states),
                        'nodes_involved': [v.log_source for v in views_at_time],
                        'views': views_at_time
                    })
        
        return {
            'detected': len(split_brain_events) > 0,
            'events': split_brain_events,
            'total_events': len(split_brain_events),
            'analysis_timestamp': datetime.now()
        }
    
    def get_authoritative_timeline(self) -> List[ViewEntity]:
        """
        Construct authoritative cluster timeline by resolving conflicts
        
        Returns:
            List of ViewEntity representing the most likely true timeline
        """
        if not self.timeline:
            return []
        
        # Use majority consensus for conflicting views
        authoritative = []
        
        # Group views by timestamp (within tolerance)
        timestamp_groups = defaultdict(list)
        tolerance_seconds = 10  # Tighter tolerance for authoritative timeline
        
        for view in self.timeline:
            if view.timestamp:
                rounded_time = datetime.fromtimestamp(
                    (view.timestamp.timestamp() // tolerance_seconds) * tolerance_seconds
                )
                timestamp_groups[rounded_time].append(view)
        
        # For each time group, select most authoritative view
        for timestamp in sorted(timestamp_groups.keys()):
            views = timestamp_groups[timestamp]
            
            if len(views) == 1:
                authoritative.append(views[0])
            else:
                # Use consensus or pick most complete view
                best_view = max(views, key=lambda v: (
                    len(v.members),  # More complete membership
                    1 if v.cluster_state == "PRIMARY" else 0,  # Prefer PRIMARY state
                    v.confidence,  # Higher confidence
                    len(v.validation_notes) == 0  # Fewer validation issues
                ))
                authoritative.append(best_view)
        
        return authoritative


@dataclass(frozen=True)
class MemberCollection:
    """
    Collection of member entities with identity correlation
    
    Handles the unstable index problem by correlating members across
    different log entries using multiple identification strategies.
    """
    
    members_by_uuid: Dict[str, List[NodeEntity]] = field(default_factory=dict)
    members_by_name: Dict[str, List[NodeEntity]] = field(default_factory=dict)
    members_by_address: Dict[str, List[NodeEntity]] = field(default_factory=dict)
    member_timeline: List[NodeEntity] = field(default_factory=list)
    
    # Identity correlation mapping
    uuid_to_names: Dict[str, Set[str]] = field(default_factory=dict)
    uuid_to_addresses: Dict[str, Set[str]] = field(default_factory=dict)
    
    def add_member(self, member: NodeEntity) -> "MemberCollection":
        """Return a new MemberCollection with the added member (immutable)"""
        from copy import deepcopy
        members_by_uuid = deepcopy(self.members_by_uuid)
        members_by_name = deepcopy(self.members_by_name)
        members_by_address = deepcopy(self.members_by_address)
        member_timeline = list(self.member_timeline)
        uuid_to_names = deepcopy(self.uuid_to_names)
        uuid_to_addresses = deepcopy(self.uuid_to_addresses)
        # Add to collections
        if member.node_id:
            if member.node_id not in members_by_uuid:
                members_by_uuid[member.node_id] = []
            members_by_uuid[member.node_id].append(member)
        if member.node_name:
            if member.node_name not in members_by_name:
                members_by_name[member.node_name] = []
            members_by_name[member.node_name].append(member)
        if member.node_address:
            if member.node_address not in members_by_address:
                members_by_address[member.node_address] = []
            members_by_address[member.node_address].append(member)
        member_timeline.append(member)
        member_timeline.sort(key=lambda m: m.timestamp or datetime.min)
        # Update correlation mappings
        if member.node_id:
            uuid_to_names.setdefault(member.node_id, set()).add(member.node_name)
            uuid_to_addresses.setdefault(member.node_id, set()).add(member.node_address)
        return MemberCollection(
            members_by_uuid=members_by_uuid,
            members_by_name=members_by_name,
            members_by_address=members_by_address,
            member_timeline=member_timeline,
            uuid_to_names=uuid_to_names,
            uuid_to_addresses=uuid_to_addresses
        )
    
    def _update_correlations(self, member: NodeEntity):
        """Update identity correlation mappings"""
        if member.node_id:
            if member.node_id not in self.uuid_to_names:
                self.uuid_to_names[member.node_id] = set()
            if member.node_id not in self.uuid_to_addresses:
                self.uuid_to_addresses[member.node_id] = set()
            
            if member.node_name:
                self.uuid_to_names[member.node_id].add(member.node_name)
            if member.node_address:
                self.uuid_to_addresses[member.node_id].add(member.node_address)
    
    def resolve_member_identity(self, partial_member: Dict[str, Any]) -> Optional[str]:
        """
        Resolve member identity from partial information
        
        Args:
            partial_member: Dict with available member information
            
        Returns:
            Resolved UUID or None if not found
        """
        # Direct UUID match
        if partial_member.get('uuid'):
            return partial_member['uuid']
        
        # Name-based correlation
        if partial_member.get('name'):
            for uuid, names in self.uuid_to_names.items():
                if partial_member['name'] in names:
                    return uuid
        
        # Address-based correlation
        if partial_member.get('address'):
            for uuid, addresses in self.uuid_to_addresses.items():
                if partial_member['address'] in addresses:
                    return uuid
        
        return None
    
    def get_member_lifecycle(self, member_uuid: str) -> List[NodeEntity]:
        """Get complete lifecycle for a specific member"""
        return self.members_by_uuid.get(member_uuid, [])


@dataclass(frozen=True)
class ClusterEntity(Entity):
    """
    Root hierarchical entity representing a complete Galera cluster
    
    This entity serves as the top-level container for all cluster-related
    entities and provides comprehensive cluster analysis capabilities.
    """
    
    entity_type: EntityType = field(default=EntityType.CLUSTER, init=False)
    
    # Cluster identification
    cluster_name: str = ""
    cluster_uuid: str = ""
    
    # Cluster lifecycle
    cluster_start_time: Optional[datetime] = None
    cluster_end_time: Optional[datetime] = None  # None means still active
    
    # Summary statistics
    max_members_seen: int = 0  # High watermark of cluster membership
    total_sst_operations: int = 0  # Count of SST operations during cluster life
    total_view_changes: int = 0  # Count of view changes during cluster life
    split_brain_events: int = 0  # Count of detected split-brain events
    error_count: int = 0  # Count of errors during cluster life
    warning_count: int = 0  # Count of warnings during cluster life
    
    # Analysis metadata
    analysis_start_time: datetime = field(default_factory=datetime.now)
    analysis_end_time: Optional[datetime] = None
    log_sources: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Initialize cluster entity"""
        # Override entity_type for cluster
        object.__setattr__(self, 'entity_type', EntityType.CLUSTER)
    
    def get_cluster_duration(self) -> Optional[timedelta]:
        """Get the duration of this cluster period"""
        if self.cluster_start_time and self.cluster_end_time:
            return self.cluster_end_time - self.cluster_start_time
        elif self.cluster_start_time:
            return datetime.now() - self.cluster_start_time
        return None
    
    def is_active(self) -> bool:
        """Check if this cluster is still active (no end time)"""
        return self.cluster_end_time is None
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """
        Get cluster summary information
        
        Returns:
            Dict with cluster summary
        """
        duration = self.get_cluster_duration()
        summary = {
            'cluster_info': {
                'name': self.cluster_name,
                'uuid': self.cluster_uuid,
                'start_time': self.cluster_start_time.isoformat() if self.cluster_start_time else None,
                'end_time': self.cluster_end_time.isoformat() if self.cluster_end_time else None,
                'duration_hours': duration.total_seconds() / 3600 if duration else None,
                'is_active': self.is_active(),
                'log_sources': self.log_sources
            },
            'statistics': {
                'max_members_seen': self.max_members_seen,
                'total_sst_operations': self.total_sst_operations,
                'total_view_changes': self.total_view_changes,
                'split_brain_events': self.split_brain_events,
                'error_count': self.error_count,
                'warning_count': self.warning_count
            }
        }
        return summary
    
    def validate(self) -> bool:
        """Validate cluster entity"""
        if not self.log_sources:
            logger.warning("Cluster has no log sources - this may indicate incomplete analysis")
        
        if not self.cluster_uuid:
            raise ValueError("Cluster entity must have a cluster UUID")
        
        return True
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for cluster entity ID generation"""
        return {
            'name': self.cluster_name or 'unknown',
            'uuid': self.cluster_uuid or 'unknown',
            'timestamp': self.cluster_start_time or self.analysis_start_time,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterEntity':
        """Create cluster entity from dictionary"""
        # Handle timestamp conversion
        analysis_start_time = datetime.now()
        if data.get('analysis_start_time'):
            try:
                analysis_start_time = datetime.fromisoformat(data['analysis_start_time'])
            except ValueError:
                pass

        analysis_end_time = None
        if data.get('analysis_end_time'):
            try:
                analysis_end_time = datetime.fromisoformat(data['analysis_end_time'])
            except ValueError:
                pass
        
        cluster_start_time = None
        if data.get('cluster_start_time'):
            try:
                cluster_start_time = datetime.fromisoformat(data['cluster_start_time'])
            except ValueError:
                pass
                
        cluster_end_time = None
        if data.get('cluster_end_time'):
            try:
                cluster_end_time = datetime.fromisoformat(data['cluster_end_time'])
            except ValueError:
                pass

        return cls(
            entity_id=data.get('entity_id', ''),
            timestamp=cluster_start_time or analysis_start_time,
            line_number=data.get('line_number'),
            raw_line=data.get('raw_line', ''),
            log_source=data.get('log_source', ''),
            confidence=data.get('confidence', 1.0),
            pattern_name=data.get('pattern_name', ''),
            extraction_method=data.get('extraction_method', 'analysis'),
            validated=data.get('validated', False),
            validation_notes=data.get('validation_notes', ''),
            cluster_name=data.get('cluster_name', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            cluster_start_time=cluster_start_time,
            cluster_end_time=cluster_end_time,
            max_members_seen=data.get('max_members_seen', 0),
            total_sst_operations=data.get('total_sst_operations', 0),
            total_view_changes=data.get('total_view_changes', 0),
            split_brain_events=data.get('split_brain_events', 0),
            error_count=data.get('error_count', 0),
            warning_count=data.get('warning_count', 0),
            analysis_start_time=analysis_start_time,
            analysis_end_time=analysis_end_time,
            log_sources=data.get('log_sources', [])
        )