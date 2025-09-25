"""
Enhanced entity architecture for proper NODE, NODE_STATE, and CLUSTER entities

This module implements the corrected entity architecture:
- ClusterEntity: Root entity representing the cluster itself
- NodeEntity: Physical node with persistent attributes (UUID, IP, ports, name)
- NodeStateEntity: State changes with timestamps and foreign key to NodeEntity
"""

from datetime import datetime
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, field
from enum import Enum

from .base import Entity, Event, EntityType
from .core import NodeState  # Reuse existing NodeState enum


@dataclass
class ClusterEntity(Entity):
    """
    Root entity representing a Galera cluster
    
    This is the top-level entity that contains cluster-wide information
    and serves as the parent for all cluster components.
    """
    
    entity_type: EntityType = field(default=EntityType.CLUSTER, init=False)
    
    # Cluster identification
    cluster_uuid: str = ""  # 8-character cluster UUID (e.g., "bbbbbbbb")
    cluster_name: str = ""  # wsrep_cluster_name
    group_name: str = ""    # Alternative cluster name field
    
    # Cluster configuration
    cluster_address: str = ""  # wsrep_cluster_address
    provider_name: str = "Galera"
    provider_version: str = ""
    provider_vendor: str = "Codership Oy"
    
    # Cluster state
    is_primary: bool = True
    total_order: Optional[int] = None  # Current TO value
    current_seqno: Optional[int] = None  # Latest sequence number
    
    # Member tracking
    known_nodes: Set[str] = field(default_factory=set)  # Set of node UUIDs
    active_nodes: Set[str] = field(default_factory=set)  # Currently active node UUIDs
    
    # Version information
    protocol_version: Optional[int] = None
    wsrep_version: str = ""
    
    def validate(self) -> bool:
        """Validate cluster entity"""
        if not self.cluster_uuid and not self.cluster_name:
            raise ValueError("Cluster must have either UUID or name")
        return True
    
    def add_node(self, node_uuid: str):
        """Add a node to the cluster's known nodes"""
        self.known_nodes.add(node_uuid)
        
    def activate_node(self, node_uuid: str):
        """Mark a node as active in the cluster"""
        self.known_nodes.add(node_uuid)
        self.active_nodes.add(node_uuid)
        
    def deactivate_node(self, node_uuid: str):
        """Mark a node as inactive in the cluster"""
        self.active_nodes.discard(node_uuid)
    
    def get_member_count(self) -> int:
        """Get total number of known cluster members"""
        return len(self.known_nodes)
    
    def get_active_member_count(self) -> int:
        """Get number of currently active cluster members"""
        return len(self.active_nodes)
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for cluster entity ID generation"""
        cluster_id = self.cluster_uuid or self.cluster_name or "default_cluster"
        return {
            'cluster_id': cluster_id,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert cluster entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'cluster_uuid': self.cluster_uuid,
            'cluster_name': self.cluster_name,
            'group_name': self.group_name,
            'cluster_address': self.cluster_address,
            'provider_name': self.provider_name,
            'provider_version': self.provider_version,
            'provider_vendor': self.provider_vendor,
            'is_primary': self.is_primary,
            'total_order': self.total_order,
            'current_seqno': self.current_seqno,
            'known_nodes': list(self.known_nodes),
            'active_nodes': list(self.active_nodes),
            'protocol_version': self.protocol_version,
            'wsrep_version': self.wsrep_version
        })
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ClusterEntity':
        """Create cluster entity from dictionary"""
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        return cls(
            entity_id=data.get('entity_id', ''),
            timestamp=timestamp,
            line_number=data.get('line_number'),
            raw_line=data.get('raw_line', ''),
            log_source=data.get('log_source', ''),
            confidence=data.get('confidence', 1.0),
            pattern_name=data.get('pattern_name', ''),
            extraction_method=data.get('extraction_method', 'manual'),
            validated=data.get('validated', False),
            validation_notes=data.get('validation_notes', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            cluster_name=data.get('cluster_name', ''),
            group_name=data.get('group_name', ''),
            cluster_address=data.get('cluster_address', ''),
            provider_name=data.get('provider_name', 'Galera'),
            provider_version=data.get('provider_version', ''),
            provider_vendor=data.get('provider_vendor', 'Codership Oy'),
            is_primary=data.get('is_primary', True),
            total_order=data.get('total_order'),
            current_seqno=data.get('current_seqno'),
            known_nodes=set(data.get('known_nodes', [])),
            active_nodes=set(data.get('active_nodes', [])),
            protocol_version=data.get('protocol_version'),
            wsrep_version=data.get('wsrep_version', '')
        )


@dataclass
class NodeEntity(Entity):
    """
    Represents a physical Galera cluster node
    
    This entity captures the persistent attributes of a node:
    - Node identification (UUID, name)
    - Network configuration (IP, ports)
    - Node configuration and capabilities
    
    State changes are tracked separately in NodeStateEntity.
    """
    
    entity_type: EntityType = field(default=EntityType.NODE, init=False)
    
    # Node identification
    node_uuid: str = ""      # Full 36-character UUID
    short_uuid: str = ""     # 8-4 character short UUID (xxxxxxxx-xxxx)
    node_name: str = ""      # Human-readable node name (wsrep_node_name)
    node_index: Optional[int] = None  # Local node index in cluster
    
    # Network configuration
    node_ip: str = ""           # Primary IP address
    mariadb_port: int = 3306    # MariaDB service port
    wsrep_port_out: int = 4567  # Galera outgoing port
    wsrep_port_in: int = 4568   # Galera incoming port (usually out+1)
    sst_port: int = 4444        # SST port (when different from wsrep_port_in)
    
    # Node configuration
    node_address: str = ""      # Full address string (IP:port)
    listen_address: str = ""    # wsrep_node_address
    data_dir: str = ""          # MySQL data directory path
    
    # Capabilities and configuration
    server_version: str = ""    # MariaDB server version
    wsrep_version: str = ""     # WSREP provider version
    galera_version: str = ""    # Galera library version
    
    # Cluster membership
    cluster_uuid: str = ""      # Parent cluster UUID (foreign key)
    is_active: bool = True      # Currently part of cluster
    
    # Current state (maintained for compatibility)
    last_known_state: NodeState = NodeState.UNKNOWN
    last_state_update: Optional[datetime] = None
    
    def validate(self) -> bool:
        """Validate node entity"""
        if not self.node_uuid and not self.node_name and not self.node_ip:
            raise ValueError("Node must have at least UUID, name, or IP address")
            
        # Validate port ranges
        if not (1 <= self.mariadb_port <= 65535):
            raise ValueError(f"Invalid MariaDB port: {self.mariadb_port}")
        if not (1 <= self.wsrep_port_out <= 65535):
            raise ValueError(f"Invalid WSREP outgoing port: {self.wsrep_port_out}")
        if not (1 <= self.wsrep_port_in <= 65535):
            raise ValueError(f"Invalid WSREP incoming port: {self.wsrep_port_in}")
            
        return True
    
    def __post_init__(self):
        """Post-initialization processing"""
        super().__post_init__()
        
        # Generate short UUID from full UUID if available
        if self.node_uuid and not self.short_uuid:
            self.generate_short_uuid()
            
        # Set default incoming port if not specified
        if self.wsrep_port_out and not self.wsrep_port_in:
            self.wsrep_port_in = self.wsrep_port_out + 1
            
        # Generate node_address if not set
        if self.node_ip and not self.node_address:
            self.node_address = f"{self.node_ip}:{self.wsrep_port_out}"
    
    def generate_short_uuid(self):
        """Generate short UUID from full UUID"""
        if self.node_uuid and len(self.node_uuid) == 36:
            # Standard UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            # Short format: xxxxxxxx-xxxx (first part + fourth part)
            parts = self.node_uuid.split('-')
            if len(parts) == 5:
                self.short_uuid = f"{parts[0]}-{parts[3]}"
    
    def update_state(self, new_state: NodeState, timestamp: Optional[datetime] = None):
        """
        Update node's last known state (for compatibility)
        
        Note: Proper state tracking should use NodeStateEntity
        """
        self.last_known_state = new_state
        self.last_state_update = timestamp or datetime.now()
    
    def get_display_name(self) -> str:
        """Get the best display name for this node"""
        if self.node_name:
            return self.node_name
        elif self.short_uuid:
            return self.short_uuid
        elif self.node_uuid:
            return self.node_uuid[:8] + "..."
        elif self.node_ip:
            return self.node_ip
        else:
            return f"node_{self.node_index}" if self.node_index is not None else "unknown"
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for node entity ID generation"""
        # For nodes, prioritize node_name for readable IDs
        if self.node_name:
            node_identifier = self.node_name
        elif self.node_ip:
            node_identifier = self.node_ip
        elif self.node_index is not None and self.node_index >= 0:
            node_identifier = f"node_{self.node_index}"
        else:
            node_identifier = "unknown"
        
        return {
            'node_name': node_identifier,
            'node_address': self.node_address,
            'timestamp': self.timestamp,
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'node_uuid': self.node_uuid,
            'short_uuid': self.short_uuid,
            'node_name': self.node_name,
            'node_index': self.node_index,
            'node_ip': self.node_ip,
            'mariadb_port': self.mariadb_port,
            'wsrep_port_out': self.wsrep_port_out,
            'wsrep_port_in': self.wsrep_port_in,
            'sst_port': self.sst_port,
            'node_address': self.node_address,
            'listen_address': self.listen_address,
            'data_dir': self.data_dir,
            'server_version': self.server_version,
            'wsrep_version': self.wsrep_version,
            'galera_version': self.galera_version,
            'cluster_uuid': self.cluster_uuid,
            'is_active': self.is_active,
            'last_known_state': self.last_known_state.value if hasattr(self.last_known_state, 'value') else str(self.last_known_state),
            'last_state_update': self.last_state_update.isoformat() if self.last_state_update else None
        })
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeEntity':
        """Create node entity from dictionary"""
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        last_state_update = None
        if data.get('last_state_update'):
            last_state_update = datetime.fromisoformat(data['last_state_update'])
            
        # Parse last known state
        last_known_state = NodeState.UNKNOWN
        if data.get('last_known_state'):
            last_known_state = NodeState(data['last_known_state'])
            
        return cls(
            entity_id=data.get('entity_id', ''),
            timestamp=timestamp,
            line_number=data.get('line_number'),
            raw_line=data.get('raw_line', ''),
            log_source=data.get('log_source', ''),
            confidence=data.get('confidence', 1.0),
            pattern_name=data.get('pattern_name', ''),
            extraction_method=data.get('extraction_method', 'manual'),
            validated=data.get('validated', False),
            validation_notes=data.get('validation_notes', ''),
            node_uuid=data.get('node_uuid', ''),
            short_uuid=data.get('short_uuid', ''),
            node_name=data.get('node_name', ''),
            node_index=data.get('node_index'),
            node_ip=data.get('node_ip', ''),
            mariadb_port=data.get('mariadb_port', 3306),
            wsrep_port_out=data.get('wsrep_port_out', 4567),
            wsrep_port_in=data.get('wsrep_port_in', 4568),
            sst_port=data.get('sst_port', 4444),
            node_address=data.get('node_address', ''),
            listen_address=data.get('listen_address', ''),
            data_dir=data.get('data_dir', ''),
            server_version=data.get('server_version', ''),
            wsrep_version=data.get('wsrep_version', ''),
            galera_version=data.get('galera_version', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            is_active=data.get('is_active', True),
            last_known_state=last_known_state,
            last_state_update=last_state_update
        )


@dataclass
class NodeStateEntity(Event):
    """
    Represents a node state change event
    
    This entity tracks state transitions with timestamps and references
    to the parent NodeEntity. This allows tracking state evolution over time.
    """
    
    entity_type: EntityType = field(default=EntityType.NODE_STATE, init=False)
    
    # Foreign key relationship
    node_uuid: str = ""         # Reference to parent NodeEntity
    node_reference: str = ""    # Alternative reference (name or short UUID)
    
    # State transition
    from_state: NodeState = NodeState.UNKNOWN
    to_state: NodeState = NodeState.UNKNOWN
    
    # Galera-specific state information
    total_order: Optional[int] = None       # TO value at state change
    sequence_number: Optional[int] = None   # Seqno at state change
    view_id: str = ""                       # View ID when state changed
    
    # Transition context
    transition_reason: str = ""     # Why the state changed
    transition_type: str = "automatic"  # automatic, manual, error, timeout
    
    # Performance metrics
    transition_duration: Optional[float] = None  # Time taken for transition (ms)
    
    def validate(self) -> bool:
        """Validate node state entity"""
        super().validate()
        
        if not self.node_uuid and not self.node_reference:
            raise ValueError("Node state must reference a node (UUID or reference)")
            
        if self.from_state == self.to_state:
            self.validation_notes += "State transition has same from/to state"
            
        return True
    
    def is_state_progression(self) -> bool:
        """Check if this represents forward state progression"""
        # Define state progression order (simplified)
        state_order = [
            NodeState.CLOSED, NodeState.OPEN, NodeState.JOINER, 
            NodeState.JOINED, NodeState.SYNCED, NodeState.DONOR
        ]
        
        try:
            from_index = state_order.index(self.from_state)
            to_index = state_order.index(self.to_state)
            return to_index > from_index
        except ValueError:
            return False  # States not in progression order
    
    def is_error_transition(self) -> bool:
        """Check if this represents an error state transition"""
        error_transitions = [
            (NodeState.SYNCED, NodeState.CLOSED),
            (NodeState.JOINED, NodeState.CLOSED),
            (NodeState.JOINER, NodeState.CLOSED),
        ]
        return (self.from_state, self.to_state) in error_transitions
    
    def get_transition_description(self) -> str:
        """Get human-readable description of the state transition"""
        from_str = self.from_state.value if hasattr(self.from_state, 'value') else str(self.from_state)
        to_str = self.to_state.value if hasattr(self.to_state, 'value') else str(self.to_state)
        return f"{from_str} → {to_str}"
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for node state entity ID generation"""
        node_ref = self.node_uuid or self.node_reference or "unknown"
        return {
            'node': node_ref,
            'timestamp': self.timestamp,
            'from_state': self.from_state.value if hasattr(self.from_state, 'value') else str(self.from_state),
            'to_state': self.to_state.value if hasattr(self.to_state, 'value') else str(self.to_state),
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node state entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'node_uuid': self.node_uuid,
            'node_reference': self.node_reference,
            'from_state': self.from_state.value if hasattr(self.from_state, 'value') else str(self.from_state),
            'to_state': self.to_state.value if hasattr(self.to_state, 'value') else str(self.to_state),
            'total_order': self.total_order,
            'sequence_number': self.sequence_number,
            'view_id': self.view_id,
            'transition_reason': self.transition_reason,
            'transition_type': self.transition_type,
            'transition_duration': self.transition_duration
        })
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeStateEntity':
        """Create node state entity from dictionary"""
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        # Parse states
        from_state = NodeState.UNKNOWN
        if data.get('from_state'):
            from_state = NodeState(data['from_state'])
            
        to_state = NodeState.UNKNOWN
        if data.get('to_state'):
            to_state = NodeState(data['to_state'])
            
        return cls(
            entity_id=data.get('entity_id', ''),
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
            duration_ms=data.get('duration_ms'),
            node_uuid=data.get('node_uuid', ''),
            node_reference=data.get('node_reference', ''),
            from_state=from_state,
            to_state=to_state,
            total_order=data.get('total_order'),
            sequence_number=data.get('sequence_number'),
            view_id=data.get('view_id', ''),
            transition_reason=data.get('transition_reason', ''),
            transition_type=data.get('transition_type', 'automatic'),
            transition_duration=data.get('transition_duration')
        )


# Add the new entity types to EntityType enum (we'll need to update base.py)
# For now, we'll use the existing types and add new ones later