"""
Cluster Entity Implementation - Root hierarchical container

This module implements the ClusterEntity as the root container for all other entities,
providing comprehensive cluster analysis with split-brain detection and multi-node 
log correlation capabilities.

Based on the comprehensive entity model design in ref/entities/CLUSTER_ENTITY.md
"""

from datetime import datetime
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


@dataclass
class ViewCollection:
    """
    Collection of views with perspective awareness for split-brain detection
    
    This class manages views from multiple nodes and can detect conflicting
    perspectives that indicate network partitions.
    """
    
    views_by_node: Dict[str, List[ViewEntity]] = field(default_factory=dict)
    views_by_id: Dict[str, List[ViewEntity]] = field(default_factory=dict)
    timeline: List[ViewEntity] = field(default_factory=list)
    
    def add_view(self, view: ViewEntity):
        """Add a view with perspective tracking"""
        node_id = view.log_source or "unknown"
        
        # Add to node-specific collection
        if node_id not in self.views_by_node:
            self.views_by_node[node_id] = []
        self.views_by_node[node_id].append(view)
        
        # Add to view ID collection
        if view.view_id:
            if view.view_id not in self.views_by_id:
                self.views_by_id[view.view_id] = []
            self.views_by_id[view.view_id].append(view)
        
        # Add to timeline (sorted by timestamp)
        self.timeline.append(view)
        self.timeline.sort(key=lambda v: v.timestamp or datetime.min)
    
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


@dataclass
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
    
    def add_member(self, member: NodeEntity):
        """Add member with identity correlation"""
        
        # Add to collections
        if member.node_id:
            if member.node_id not in self.members_by_uuid:
                self.members_by_uuid[member.node_id] = []
            self.members_by_uuid[member.node_id].append(member)
        
        if member.node_name:
            if member.node_name not in self.members_by_name:
                self.members_by_name[member.node_name] = []
            self.members_by_name[member.node_name].append(member)
        
        if member.node_address:
            if member.node_address not in self.members_by_address:
                self.members_by_address[member.node_address] = []
            self.members_by_address[member.node_address].append(member)
        
        # Add to timeline
        self.member_timeline.append(member)
        self.member_timeline.sort(key=lambda m: m.timestamp or datetime.min)
        
        # Update correlation mappings
        self._update_correlations(member)
    
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


@dataclass
class ClusterEntity(Entity):
    """
    Root hierarchical entity representing a complete Galera cluster
    
    This entity serves as the top-level container for all cluster-related
    entities and provides comprehensive cluster analysis capabilities.
    """
    
    entity_type: EntityType = field(default=EntityType.NODE, init=False)  # Will be overridden
    
    # Cluster identification
    cluster_name: str = ""
    cluster_uuid: str = ""
    
    # Analysis metadata
    analysis_start_time: datetime = field(default_factory=datetime.now)
    analysis_end_time: Optional[datetime] = None
    log_sources: List[str] = field(default_factory=list)
    
    # Entity collections (hierarchical properties)
    views: ViewCollection = field(default_factory=ViewCollection)
    members: MemberCollection = field(default_factory=MemberCollection)
    
    # Flat entity collections for other types
    sst_operations: List[StateTransferEntity] = field(default_factory=list)
    communications: List[CommunicationEntity] = field(default_factory=list)
    warnings: List[WarningEntity] = field(default_factory=list)
    errors: List[ErrorEntity] = field(default_factory=list)
    performance_events: List[PerformanceEntity] = field(default_factory=list)
    transactions: List[TransactionEntity] = field(default_factory=list)
    
    # Analysis results
    cluster_health_score: Optional[float] = None
    split_brain_detected: bool = False
    split_brain_analysis: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize cluster entity"""
        # Override entity_type for cluster
        object.__setattr__(self, 'entity_type', 'CLUSTER')
        # Skip validation during initialization - will be called manually after adding entities
        self.validated = False
    
    def add_entity(self, entity: Entity):
        """
        Add any entity to the appropriate collection
        
        Args:
            entity: Entity to add to cluster
        """
        # Track log source
        if entity.log_source and entity.log_source not in self.log_sources:
            self.log_sources.append(entity.log_source)
        
        # Route to appropriate collection based on entity type
        entity_type_str = entity.entity_type.value if hasattr(entity.entity_type, 'value') else str(entity.entity_type)
        
        if entity_type_str == 'VIEW':
            self.views.add_view(entity)
        elif entity_type_str == 'NODE':
            self.members.add_member(entity)
        elif entity_type_str == 'STATE_TRANSFER':
            self.sst_operations.append(entity)
        elif entity_type_str == 'COMMUNICATION':
            self.communications.append(entity)
        elif entity_type_str == 'WARNING':
            self.warnings.append(entity)
        elif entity_type_str == 'ERROR':
            self.errors.append(entity)
        elif entity_type_str == 'PERFORMANCE':
            self.performance_events.append(entity)
        elif entity_type_str == 'TRANSACTION':
            self.transactions.append(entity)
        else:
            logger.warning(f"Unknown entity type: {entity_type_str}")
    
    def analyze_cluster_health(self) -> float:
        """
        Perform comprehensive cluster health analysis
        
        Returns:
            float: Health score from 0.0 (critical) to 1.0 (excellent)
        """
        score_components = []
        
        # View stability (30% weight)
        view_score = self._analyze_view_stability()
        score_components.append(('views', view_score, 0.30))
        
        # Error frequency (25% weight)
        error_score = self._analyze_error_frequency()
        score_components.append(('errors', error_score, 0.25))
        
        # SST frequency (20% weight)
        sst_score = self._analyze_sst_health()
        score_components.append(('sst', sst_score, 0.20))
        
        # Communication health (15% weight)
        comm_score = self._analyze_communication_health()
        score_components.append(('communication', comm_score, 0.15))
        
        # Performance indicators (10% weight)
        perf_score = self._analyze_performance_health()
        score_components.append(('performance', perf_score, 0.10))
        
        # Calculate weighted average
        total_weight = sum(weight for _, _, weight in score_components)
        weighted_score = sum(score * weight for _, score, weight in score_components) / total_weight
        
        self.cluster_health_score = weighted_score
        return weighted_score
    
    def _analyze_view_stability(self) -> float:
        """Analyze cluster view stability"""
        if not self.views.timeline:
            return 0.5  # Neutral score if no view data
        
        # Check for frequent view changes (instability indicator)
        view_count = len(self.views.timeline)
        time_span = self._get_analysis_time_span()
        
        if time_span.total_seconds() == 0:
            return 0.5
        
        # Views per hour - fewer is better for stability
        views_per_hour = view_count / (time_span.total_seconds() / 3600)
        
        # Score: < 1 view/hour = excellent, > 10 views/hour = poor
        if views_per_hour <= 1:
            stability_score = 1.0
        elif views_per_hour >= 10:
            stability_score = 0.0
        else:
            stability_score = max(0.0, 1.0 - (views_per_hour - 1) / 9)
        
        # Check for split-brain detection
        split_brain_result = self.views.detect_split_brain()
        if split_brain_result['detected']:
            stability_score *= 0.3  # Severe penalty for split-brain
            self.split_brain_detected = True
            self.split_brain_analysis = split_brain_result
        
        return stability_score
    
    def _analyze_error_frequency(self) -> float:
        """Analyze error frequency and severity"""
        if not self.errors:
            return 1.0  # Perfect score if no errors
        
        time_span = self._get_analysis_time_span()
        if time_span.total_seconds() == 0:
            return 0.5
        
        # Errors per hour
        errors_per_hour = len(self.errors) / (time_span.total_seconds() / 3600)
        
        # Score: 0 errors/hour = perfect, > 50 errors/hour = critical
        if errors_per_hour == 0:
            return 1.0
        elif errors_per_hour >= 50:
            return 0.0
        else:
            return max(0.0, 1.0 - errors_per_hour / 50)
    
    def _analyze_sst_health(self) -> float:
        """Analyze SST operation health"""
        if not self.sst_operations:
            return 1.0  # Perfect if no SSTs needed
        
        # Check SST success rate
        successful_ssts = sum(1 for sst in self.sst_operations if sst.is_successful())
        success_rate = successful_ssts / len(self.sst_operations)
        
        # Check SST frequency (fewer is better)
        time_span = self._get_analysis_time_span()
        if time_span.total_seconds() > 0:
            ssts_per_day = len(self.sst_operations) / (time_span.total_seconds() / 86400)
            frequency_score = max(0.0, 1.0 - ssts_per_day / 10)  # > 10/day is poor
        else:
            frequency_score = 0.5
        
        # Combine success rate and frequency
        return (success_rate * 0.7 + frequency_score * 0.3)
    
    def _analyze_communication_health(self) -> float:
        """Analyze cluster communication health"""
        if not self.communications:
            return 0.8  # Good score if no communication issues logged
        
        # Analyze communication patterns for issues
        connection_failures = sum(
            1 for comm in self.communications 
            if 'fail' in comm.status.lower() or 'error' in comm.status.lower()
        )
        
        if len(self.communications) == 0:
            return 0.8
        
        failure_rate = connection_failures / len(self.communications)
        return max(0.0, 1.0 - failure_rate)
    
    def _analyze_performance_health(self) -> float:
        """Analyze performance indicators"""
        if not self.performance_events:
            return 0.8  # Neutral score if no performance data
        
        # Check for performance alerts
        alerts = sum(
            1 for perf in self.performance_events 
            if perf.alert_level in ['warning', 'critical']
        )
        
        alert_rate = alerts / len(self.performance_events)
        return max(0.0, 1.0 - alert_rate)
    
    def _get_analysis_time_span(self) -> datetime:
        """Get the time span of the analysis"""
        if self.analysis_end_time:
            return self.analysis_end_time - self.analysis_start_time
        else:
            return datetime.now() - self.analysis_start_time
    
    def get_cluster_timeline(self) -> List[Entity]:
        """
        Get comprehensive cluster timeline with all entities
        
        Returns:
            List of all entities sorted by timestamp
        """
        all_entities = []
        
        # Add all entity types
        all_entities.extend(self.views.timeline)
        all_entities.extend(self.members.member_timeline)
        all_entities.extend(self.sst_operations)
        all_entities.extend(self.communications)
        all_entities.extend(self.warnings)
        all_entities.extend(self.errors)
        all_entities.extend(self.performance_events)
        all_entities.extend(self.transactions)
        
        # Sort by timestamp
        all_entities.sort(key=lambda e: e.timestamp or datetime.min)
        
        return all_entities
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive cluster analysis summary
        
        Returns:
            Dict with complete cluster analysis
        """
        timeline = self.get_cluster_timeline()
        health_score = self.analyze_cluster_health()
        
        summary = {
            'cluster_info': {
                'name': self.cluster_name,
                'uuid': self.cluster_uuid,
                'log_sources': self.log_sources,
                'analysis_period': {
                    'start': self.analysis_start_time.isoformat() if self.analysis_start_time else None,
                    'end': self.analysis_end_time.isoformat() if self.analysis_end_time else None,
                    'duration_hours': self._get_analysis_time_span().total_seconds() / 3600
                }
            },
            'health_analysis': {
                'overall_score': health_score,
                'score_interpretation': self._interpret_health_score(health_score),
                'split_brain_detected': self.split_brain_detected,
                'split_brain_analysis': self.split_brain_analysis
            },
            'entity_counts': {
                'total_entities': len(timeline),
                'views': len(self.views.timeline),
                'members': len(self.members.member_timeline),
                'sst_operations': len(self.sst_operations),
                'communications': len(self.communications),
                'warnings': len(self.warnings),
                'errors': len(self.errors),
                'performance_events': len(self.performance_events),
                'transactions': len(self.transactions)
            },
            'key_findings': self._generate_key_findings(),
            'recommendations': self._generate_recommendations()
        }
        
        return summary
    
    def _interpret_health_score(self, score: float) -> str:
        """Interpret health score as human-readable status"""
        if score >= 0.9:
            return "Excellent"
        elif score >= 0.8:
            return "Good"
        elif score >= 0.6:
            return "Fair"
        elif score >= 0.4:
            return "Poor"
        else:
            return "Critical"
    
    def _generate_key_findings(self) -> List[str]:
        """Generate key findings from cluster analysis"""
        findings = []
        
        if self.split_brain_detected:
            findings.append(f"Split-brain detected: {len(self.split_brain_analysis.get('events', []))} events")
        
        if len(self.errors) > 0:
            findings.append(f"Found {len(self.errors)} error events requiring attention")
        
        if len(self.sst_operations) > 0:
            successful_ssts = sum(1 for sst in self.sst_operations if sst.is_successful())
            findings.append(f"SST Operations: {successful_ssts}/{len(self.sst_operations)} successful")
        
        if len(self.views.timeline) > 10:
            findings.append(f"High view change frequency: {len(self.views.timeline)} view changes detected")
        
        return findings
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if self.split_brain_detected:
            recommendations.append("Investigate network connectivity between nodes")
            recommendations.append("Review cluster configuration for proper quorum settings")
        
        if self.cluster_health_score and self.cluster_health_score < 0.6:
            recommendations.append("Cluster health is below acceptable levels - immediate attention required")
        
        if len(self.errors) > len(self.views.timeline):
            recommendations.append("Error frequency exceeds view changes - investigate underlying issues")
        
        failed_ssts = [sst for sst in self.sst_operations if not sst.is_successful()]
        if len(failed_ssts) > 0:
            recommendations.append(f"Review {len(failed_ssts)} failed SST operations for root cause")
        
        return recommendations
    
    def validate(self) -> bool:
        """Validate cluster entity"""
        if not self.log_sources:
            logger.warning("Cluster has no log sources - this may indicate incomplete analysis")
        
        # Validate that we have at least some entities
        total_entities = (len(self.views.timeline) + len(self.members.member_timeline) + 
                         len(self.sst_operations) + len(self.communications) + 
                         len(self.warnings) + len(self.errors) + 
                         len(self.performance_events) + len(self.transactions))
        
        if total_entities == 0:
            raise ValueError("Cluster entity must contain at least one child entity")
        
        return True
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for cluster entity ID generation"""
        return {
            'name': self.cluster_name or 'unknown',
            'uuid': self.cluster_uuid or 'unknown',
            'timestamp': self.analysis_start_time,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert cluster entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'cluster_name': self.cluster_name,
            'cluster_uuid': self.cluster_uuid,
            'analysis_start_time': self.analysis_start_time.isoformat() if self.analysis_start_time else None,
            'analysis_end_time': self.analysis_end_time.isoformat() if self.analysis_end_time else None,
            'log_sources': self.log_sources,
            'entity_counts': {
                'views': len(self.views.timeline),
                'members': len(self.members.member_timeline),
                'sst_operations': len(self.sst_operations),
                'communications': len(self.communications),
                'warnings': len(self.warnings),
                'errors': len(self.errors),
                'performance_events': len(self.performance_events),
                'transactions': len(self.transactions)
            },
            'cluster_health_score': self.cluster_health_score,
            'split_brain_detected': self.split_brain_detected,
            'split_brain_analysis': self.split_brain_analysis
        })
        return base_dict
    
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
        
        return cls(
            entity_id=data.get('entity_id', ''),
            timestamp=analysis_start_time,
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
            analysis_start_time=analysis_start_time,
            analysis_end_time=analysis_end_time,
            log_sources=data.get('log_sources', []),
            cluster_health_score=data.get('cluster_health_score'),
            split_brain_detected=data.get('split_brain_detected', False),
            split_brain_analysis=data.get('split_brain_analysis', {})
        )