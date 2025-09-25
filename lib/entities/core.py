"""
Core entity classes for Galera cluster log analysis

This module implements the specific entity types used in Galera cluster analysis:
- NodeEntity: Represents a cluster node and its state
- StateTransferEntity: Represents SST/IST operations
- ViewEntity: Represents cluster membership changes
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum

from .base import Entity, Event, EntityType
from .temporal import TemporalProperty, TemporalPropertySet


class NodeState(Enum):
    """Galera node states"""
    OPEN = "OPEN"
    PRIMARY = "PRIMARY"
    JOINER = "JOINER"
    JOINED = "JOINED"
    SYNCED = "SYNCED"
    DONOR = "DONOR"
    DESYNCED = "DESYNCED"
    DESTROYED = "DESTROYED"
    CLOSED = "CLOSED"
    UNKNOWN = "UNKNOWN"


class StateTransferType(Enum):
    """State transfer types"""
    SST = "SST"  # State Snapshot Transfer
    IST = "IST"  # Incremental State Transfer


class StateTransferMethod(Enum):
    """State transfer methods"""
    RSYNC = "rsync"
    MYSQLDUMP = "mysqldump"
    XTRABACKUP = "xtrabackup"
    MARIABACKUP = "mariabackup"
    CLONE = "clone"


@dataclass
class NodeEntity(Entity):
    """
    Represents a Galera cluster node
    
    This entity captures information about individual nodes in the cluster,
    including their state, configuration, and role changes.
    """
    
    entity_type: EntityType = field(default=EntityType.NODE, init=False)
    
    # Log level (Note, Warning, Error, etc.)
    level: str = "Note"
    
    # Node identification
    node_uuid: str = ""  # Full Galera node UUID 
    short_uuid: str = ""  # Short UUID format (parts 1 and 4)
    node_name: str = ""  # Human-readable node name
    node_address: str = ""  # IP:port combination
    state_uuid: str = ""  # State exchange UUID (when available)
    
    # Relationship aliases (for efficient lookups)
    uuid_alias: str = field(default="", init=False)  # Short UUID for direct relationship matching
    
    # Node state
    current_state: NodeState = NodeState.UNKNOWN
    previous_state: Optional[NodeState] = None
    
    # Cluster information
    cluster_name: str = ""
    cluster_uuid: str = ""
    
    # Version information
    wsrep_version: str = ""
    galera_version: str = ""
    mariadb_version: str = ""
    
    # Performance metrics (when available)
    local_index: Optional[int] = None
    local_cached_downto: Optional[int] = None
    
    def validate(self) -> bool:
        """Validate node entity data"""
        if not self.short_uuid and not self.node_name and not self.node_address:
            raise ValueError("Node must have at least one identifier (uuid, name, or address)")
            
        # Validate state transition if both states are present
        if self.previous_state and self.current_state:
            valid_transitions = self._get_valid_transitions()
            if (self.previous_state, self.current_state) not in valid_transitions:
                # Log warning but don't fail validation (state might be incomplete)
                prev_state_str = self.previous_state.value if hasattr(self.previous_state, 'value') else str(self.previous_state)
                current_state_str = self.current_state.value if hasattr(self.current_state, 'value') else str(self.current_state)
                self.validation_notes += f"Unusual state transition: {prev_state_str} -> {current_state_str}"
                
        return True
    
    def convert_long_uuid_to_short(self) -> None:
        """Convert long UUID format to short format if available"""
        if self.node_uuid and not self.short_uuid:
            # Long UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            # Short UUID format: xxxxxxxx-xxxx (1st part + 4th part)
            parts = self.node_uuid.split('-')
            if len(parts) == 5:
                self.short_uuid = f"{parts[0]}-{parts[3]}"
    
    def __post_init__(self):
        """Post-initialization processing"""
        super().__post_init__()
        # Convert long UUID to short format if needed
        self.convert_long_uuid_to_short()
        # Set UUID alias for relationship matching
        self.uuid_alias = self.short_uuid if self.short_uuid else ""
        
    def _get_valid_transitions(self) -> List[tuple]:
        """Get list of valid Galera state transitions"""
        return [
            # Normal startup sequence
            (NodeState.CLOSED, NodeState.OPEN),
            (NodeState.OPEN, NodeState.PRIMARY),
            (NodeState.OPEN, NodeState.JOINER),
            (NodeState.JOINER, NodeState.JOINED),
            (NodeState.JOINED, NodeState.SYNCED),
            
            # Donor transitions
            (NodeState.SYNCED, NodeState.DONOR),
            (NodeState.DONOR, NodeState.SYNCED),
            (NodeState.DONOR, NodeState.DESYNCED),
            (NodeState.DESYNCED, NodeState.SYNCED),
            
            # Shutdown sequence
            (NodeState.SYNCED, NodeState.CLOSED),
            (NodeState.PRIMARY, NodeState.CLOSED),
            (NodeState.OPEN, NodeState.CLOSED),
            
            # Error states
            (NodeState.UNKNOWN, NodeState.OPEN),
            (NodeState.UNKNOWN, NodeState.CLOSED),
            
            # Destroyed state (can come from anywhere)
            (NodeState.SYNCED, NodeState.DESTROYED),
            (NodeState.DONOR, NodeState.DESTROYED),
            (NodeState.JOINER, NodeState.DESTROYED),
            (NodeState.JOINED, NodeState.DESTROYED),
            (NodeState.DESYNCED, NodeState.DESTROYED),
            (NodeState.PRIMARY, NodeState.DESTROYED),
            (NodeState.OPEN, NodeState.DESTROYED),
        ]
        
    def update_state(self, new_state: NodeState, timestamp: Optional[datetime] = None):
        """
        Update node state with transition tracking
        
        Args:
            new_state: New node state
            timestamp: Optional timestamp of state change
        """
        self.previous_state = self.current_state
        self.current_state = new_state
        
        if timestamp:
            self.timestamp = timestamp
            
        # Update validation notes
        if self.previous_state and self.previous_state != new_state:
            prev_state_str = self.previous_state.value if hasattr(self.previous_state, 'value') else str(self.previous_state)
            new_state_str = new_state.value if hasattr(new_state, 'value') else str(new_state)
            transition = f"{prev_state_str} -> {new_state_str}"
            if self.validation_notes:
                self.validation_notes += f"; State transition: {transition}"
            else:
                self.validation_notes = f"State transition: {transition}"
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for node entity ID generation"""
        # For nodes, use node_name or fallback to node_address
        if self.node_name:
            node_identifier = self.node_name
        elif self.node_address:
            node_identifier = self.node_address
        elif self.local_index is not None and self.local_index >= 0:
            node_identifier = f"node_{self.local_index}"
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
            'level': self.level,
            'node_uuid': self.node_uuid,
            'short_uuid': self.short_uuid,
            'node_name': self.node_name,
            'node_address': self.node_address,
            'state_uuid': self.state_uuid,
            'current_state': self.current_state.value if hasattr(self.current_state, 'value') else str(self.current_state),
            'previous_state': self.previous_state.value if self.previous_state and hasattr(self.previous_state, 'value') else str(self.previous_state) if self.previous_state else None,
            'cluster_name': self.cluster_name,
            'cluster_uuid': self.cluster_uuid,
            'wsrep_version': self.wsrep_version,
            'galera_version': self.galera_version,
            'mariadb_version': self.mariadb_version,
            'local_index': self.local_index,
            'local_cached_downto': self.local_cached_downto
        })
        return base_dict
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeEntity':
        """Create node entity from dictionary"""
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        # Parse states
        current_state = NodeState.UNKNOWN
        if data.get('current_state'):
            current_state = NodeState(data['current_state'])
            
        previous_state = None
        if data.get('previous_state'):
            previous_state = NodeState(data['previous_state'])
            
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
            level=data.get('level', 'Note'),
            node_uuid=data.get('node_uuid', ''),
            short_uuid=data.get('short_uuid', ''),
            node_name=data.get('node_name', ''),
            node_address=data.get('node_address', ''),
            state_uuid=data.get('state_uuid', ''),
            current_state=current_state,
            previous_state=previous_state,
            cluster_name=data.get('cluster_name', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            wsrep_version=data.get('wsrep_version', ''),
            galera_version=data.get('galera_version', ''),
            mariadb_version=data.get('mariadb_version', ''),
            local_index=data.get('local_index'),
            local_cached_downto=data.get('local_cached_downto')
        )


@dataclass 
class StateTransferEntity(Event):
    """
    Represents a state transfer operation (SST or IST)
    
    This entity captures information about state synchronization operations
    between cluster nodes.
    """
    
    entity_type: EntityType = field(default=EntityType.STATE_TRANSFER, init=False)
    
    # Transfer identification
    transfer_type: StateTransferType = StateTransferType.SST
    transfer_method: Optional[StateTransferMethod] = None
    
    # Node information
    donor_node: str = ""  # Node providing the state
    joiner_node: str = ""  # Node receiving the state
    donor_address: str = ""
    joiner_address: str = ""
    
    # Transfer details
    transfer_status: str = "unknown"  # started, completed, failed, cancelled
    transferred_bytes: Optional[int] = None
    transfer_rate: Optional[float] = None  # bytes per second
    
    # Galera-specific details
    seqno_start: Optional[int] = None  # Starting sequence number
    seqno_end: Optional[int] = None    # Ending sequence number
    uuid: str = ""  # State UUID
    
    # Error information
    error_code: Optional[int] = None
    error_message: str = ""
    
    def validate(self) -> bool:
        """Validate state transfer entity"""
        # Call parent validation
        super().validate()
        
        # Allow creation with minimal information - at least one field should be present
        if (not self.donor_node and not self.joiner_node and 
            not self.transfer_status and not self.transfer_type):
            raise ValueError("State transfer must have at least one identifying field")
            
        if self.transferred_bytes is not None and self.transferred_bytes < 0:
            raise ValueError("Transferred bytes cannot be negative")
            
        if self.transfer_rate is not None and self.transfer_rate < 0:
            raise ValueError("Transfer rate cannot be negative")
            
        if self.seqno_start is not None and self.seqno_end is not None:
            if self.seqno_start > self.seqno_end:
                raise ValueError("Start sequence number cannot be greater than end sequence number")
                
        return True
        
    def calculate_duration(self) -> Optional[float]:
        """
        Calculate transfer duration if possible
        
        Returns:
            float: Duration in milliseconds, or None if cannot be calculated
        """
        if self.duration_ms is not None:
            return self.duration_ms
            
        # Try to calculate from transfer rate and size
        if self.transferred_bytes and self.transfer_rate:
            duration_seconds = self.transferred_bytes / self.transfer_rate
            return duration_seconds * 1000
            
        return None
        
    def is_successful(self) -> bool:
        """Check if the state transfer was successful"""
        return (self.transfer_status.lower() in ['completed', 'success', 'done'] and 
                self.error_code is None)
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for SST entity ID generation"""
        return {
            'timestamp': self.timestamp,
            'donor': self.donor_node or self.donor_address or "unknown",
            'joiner': self.joiner_node or self.joiner_address or "unknown",
        }
                
    def to_dict(self) -> Dict[str, Any]:
        """Convert state transfer entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'transfer_type': self.transfer_type.value if hasattr(self.transfer_type, 'value') else str(self.transfer_type),
            'transfer_method': self.transfer_method.value if self.transfer_method and hasattr(self.transfer_method, 'value') else str(self.transfer_method) if self.transfer_method else None,
            'donor_node': self.donor_node,
            'joiner_node': self.joiner_node,
            'donor_address': self.donor_address,
            'joiner_address': self.joiner_address,
            'transfer_status': self.transfer_status,
            'transferred_bytes': self.transferred_bytes,
            'transfer_rate': self.transfer_rate,
            'seqno_start': self.seqno_start,
            'seqno_end': self.seqno_end,
            'uuid': self.uuid,
            'error_code': self.error_code,
            'error_message': self.error_message
        })
        return base_dict
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StateTransferEntity':
        """Create state transfer entity from dictionary"""
        timestamp = None
        if data.get('timestamp'):
            timestamp = datetime.fromisoformat(data['timestamp'])
            
        # Parse transfer type
        transfer_type = StateTransferType.SST
        if data.get('transfer_type'):
            transfer_type = StateTransferType(data['transfer_type'])
            
        # Parse transfer method
        transfer_method = None
        if data.get('transfer_method'):
            transfer_method = StateTransferMethod(data['transfer_method'])
            
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
            transfer_type=transfer_type,
            transfer_method=transfer_method,
            donor_node=data.get('donor_node', ''),
            joiner_node=data.get('joiner_node', ''),
            donor_address=data.get('donor_address', ''),
            joiner_address=data.get('joiner_address', ''),
            transfer_status=data.get('transfer_status', 'unknown'),
            transferred_bytes=data.get('transferred_bytes'),
            transfer_rate=data.get('transfer_rate'),
            seqno_start=data.get('seqno_start'),
            seqno_end=data.get('seqno_end'),
            uuid=data.get('uuid', ''),
            error_code=data.get('error_code'),
            error_message=data.get('error_message', '')
        )


@dataclass
class ViewEntity(Event):
    """
    Represents a cluster view change event
    
    This entity captures information about cluster membership changes,
    including nodes joining or leaving the cluster.
    """
    
    entity_type: EntityType = field(default=EntityType.VIEW, init=False)
    
    # Log level (Note, Warning, Error, etc.)
    level: str = "Note"
    
    # View identification
    view_id: str = ""  # Unique view identifier
    view_seq: Optional[int] = None  # Sequential view number
    
    # Cluster state
    cluster_uuid: str = ""
    cluster_state: str = "unknown"  # PRIMARY, NON_PRIMARY, etc.
    state: str = ""  # Alternative field name for cluster_state
    
    # Membership information
    members: List[str] = field(default_factory=list)  # Node UUIDs
    member_addresses: List[str] = field(default_factory=list)  # IP:port
    
    # View changes
    joined_nodes: List[str] = field(default_factory=list)
    left_nodes: List[str] = field(default_factory=list)
    
    # Protocol information
    protocol_version: Optional[int] = None
    evs_protocol_version: Optional[int] = None
    
    def __post_init__(self):
        """Post-initialization processing"""
        super().__post_init__()
        
        # Map 'state' to 'cluster_state' if provided
        if self.state and not self.cluster_state:
            self.cluster_state = self.state
        elif self.state and self.cluster_state == "unknown":
            self.cluster_state = self.state
    
    def validate(self) -> bool:
        """Validate view entity"""
        # Call parent validation
        super().validate()
        
        if not self.view_id and self.view_seq is None:
            raise ValueError("View must have either view_id or view_seq")
            
        # Ensure member lists have same length if both present
        if (self.members and self.member_addresses and 
            len(self.members) != len(self.member_addresses)):
            self.validation_notes += "Member count mismatch between UUIDs and addresses"
            
        return True
        
    def get_member_count(self) -> int:
        """Get number of cluster members"""
        return len(self.members)
        
    def is_primary_view(self) -> bool:
        """Check if this is a primary cluster view"""
        return self.cluster_state.upper() == "PRIMARY"
        
    def get_membership_change(self) -> Dict[str, List[str]]:
        """
        Get summary of membership changes
        
        Returns:
            Dict with 'joined' and 'left' node lists
        """
        return {
            'joined': self.joined_nodes.copy(),
            'left': self.left_nodes.copy()
        }
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for view entity ID generation"""
        # Use (node_short_uuid,seqno) format to prevent duplicates
        node_short_uuid = self.view_id if self.view_id else "unknown"
        seqno = str(self.view_seq) if self.view_seq is not None else "0"
        view_identifier = f"({node_short_uuid},{seqno})"
        return {
            'id': view_identifier,
            'timestamp': self.timestamp,
        }
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert view entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'view_id': self.view_id,
            'view_seq': self.view_seq,
            'cluster_uuid': self.cluster_uuid,
            'cluster_state': self.cluster_state,
            'members': self.members,
            'member_addresses': self.member_addresses,
            'joined_nodes': self.joined_nodes,
            'left_nodes': self.left_nodes,
            'protocol_version': self.protocol_version,
            'evs_protocol_version': self.evs_protocol_version
        })
        return base_dict
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ViewEntity':
        """Create view entity from dictionary"""
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
            event_name=data.get('event_name', ''),
            event_category=data.get('event_category', ''),
            before_state=data.get('before_state'),
            after_state=data.get('after_state'),
            related_entities=data.get('related_entities', []),
            duration_ms=data.get('duration_ms'),
            view_id=data.get('view_id', ''),
            view_seq=data.get('view_seq'),
            cluster_uuid=data.get('cluster_uuid', ''),
            cluster_state=data.get('cluster_state', 'unknown'),
            members=data.get('members', []),
            member_addresses=data.get('member_addresses', []),
            joined_nodes=data.get('joined_nodes', []),
            left_nodes=data.get('left_nodes', []),
            protocol_version=data.get('protocol_version'),
            evs_protocol_version=data.get('evs_protocol_version')
        )


@dataclass
class WsrepViewEntity(Event):
    """
    Represents a WSREP layer cluster view event
    
    This entity captures information about WSREP layer views which contain
    cluster UUID + view sequence number (actual GTID seqno) and provide
    complete membership details with capabilities.
    """
    
    entity_type: EntityType = field(default=EntityType.WSREP_VIEW, init=False)
    
    # Log level (Note, Warning, Error, etc.)
    level: str = "Note"
    
    # WSREP View identification (cluster_uuid:seqno format)
    wsrep_view_id: str = ""  # Full cluster_uuid:seqno identifier
    cluster_uuid: str = ""   # Cluster UUID part
    view_seqno: Optional[int] = None  # WSREP view sequence number (GTID seqno)
    
    # View status and capabilities
    status: str = "unknown"  # primary, non_primary, etc.
    protocol_version: Optional[int] = None
    capabilities: List[str] = field(default_factory=list)
    final: bool = False
    own_index: Optional[int] = None
    
    # Membership information (detailed format)
    members: List[Dict[str, str]] = field(default_factory=list)  # List of member dicts with index, uuid, name
    member_count: Optional[int] = None
    member_uuids: List[str] = field(default_factory=list)  # List of member UUIDs for easy access
    
    def __post_init__(self):
        """Post-initialization processing"""
        super().__post_init__()
        
        # Extract cluster_uuid and view_seqno from wsrep_view_id if provided
        if self.wsrep_view_id and ":" in self.wsrep_view_id:
            parts = self.wsrep_view_id.split(":", 1)
            if len(parts) == 2:
                self.cluster_uuid = parts[0]
                try:
                    self.view_seqno = int(parts[1])
                except ValueError:
                    pass
    
    def validate(self) -> bool:
        """Validate WSREP view entity"""
        # Call parent validation
        super().validate()
        
        if not self.wsrep_view_id and not self.cluster_uuid:
            raise ValueError("WSREP view must have wsrep_view_id or cluster_uuid")
            
        # Set member_count from members list if not provided
        if self.member_count is None and self.members:
            self.member_count = len(self.members)
            
        return True
        
    def is_primary_view(self) -> bool:
        """Check if this is a primary cluster view"""
        return self.status.lower() == "primary"
        
    def get_member_by_index(self, index: int) -> Optional[Dict[str, str]]:
        """Get member information by index"""
        for member in self.members:
            if member.get('index') == str(index):
                return member
        return None
        
    def get_member_names(self) -> List[str]:
        """Get list of member names"""
        return [member.get('name', '') for member in self.members if member.get('name')]
        
    def get_member_uuids(self) -> List[str]:
        """Get list of member UUIDs"""
        return [member.get('uuid', '') for member in self.members if member.get('uuid')]
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for WSREP view entity ID generation"""
        # Use cluster_uuid:seqno format for ID
        wsrep_identifier = self.wsrep_view_id if self.wsrep_view_id else f"{self.cluster_uuid}:{self.view_seqno or 0}"
        return {
            'id': wsrep_identifier,
            'timestamp': self.timestamp,
        }
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert WSREP view entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'wsrep_view_id': self.wsrep_view_id,
            'cluster_uuid': self.cluster_uuid,
            'view_seqno': self.view_seqno,
            'status': self.status,
            'protocol_version': self.protocol_version,
            'capabilities': self.capabilities,
            'final': self.final,
            'own_index': self.own_index,
            'members': self.members,
            'member_count': self.member_count,
            'member_uuids': self.member_uuids
        })
        return base_dict
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WsrepViewEntity':
        """Create WSREP view entity from dictionary"""
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
            event_name=data.get('event_name', ''),
            event_category=data.get('event_category', ''),
            before_state=data.get('before_state'),
            after_state=data.get('after_state'),
            related_entities=data.get('related_entities', []),
            duration_ms=data.get('duration_ms'),
            wsrep_view_id=data.get('wsrep_view_id', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            view_seqno=data.get('view_seqno'),
            status=data.get('status', 'unknown'),
            protocol_version=data.get('protocol_version'),
            capabilities=data.get('capabilities', []),
            final=data.get('final', False),
            own_index=data.get('own_index'),
            members=data.get('members', []),
            member_count=data.get('member_count'),
            member_uuids=data.get('member_uuids', [])
        )


@dataclass
class CommunicationEntity(Entity):
    """
    Represents cluster communication events like node connections and state exchanges
    """
    
    # Entity classification
    entity_type: EntityType = field(default=EntityType.COMMUNICATION, init=False)
    
    # Communication participants
    source_node: str = ""
    target_node: str = ""
    target_address: str = ""
    
    # Communication details
    communication_type: str = ""  # "connection", "state_exchange", "cleanup", etc.
    exchange_action: str = ""     # "sent", "got", "established", "stable"
    state_uuid: str = ""
    source_index: str = ""
    source_name: str = ""
    
    # Message information
    message: str = ""
    status: str = ""
    
    def validate(self) -> bool:
        """Validate communication entity"""
        if not self.communication_type:
            # Infer from pattern name or message
            if "connection" in self.pattern_name.lower():
                self.communication_type = "connection"
            elif "state_exchange" in self.pattern_name.lower():
                self.communication_type = "state_exchange"
            elif "cleanup" in self.pattern_name.lower():
                self.communication_type = "cleanup"
            elif "stable" in self.pattern_name.lower():
                self.communication_type = "stability"
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'source_node': self.source_node,
            'target_node': self.target_node,
            'target_address': self.target_address,
            'communication_type': self.communication_type,
            'exchange_action': self.exchange_action,
            'state_uuid': self.state_uuid,
            'source_index': self.source_index,
            'source_name': self.source_name,
            'message': self.message,
            'status': self.status
        })
        return base_dict

    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for communication entity ID generation"""
        return {
            'source': self.source_node or 'unknown',
            'target': self.target_node or 'unknown', 
            'type': self.communication_type or 'unknown',
            'timestamp': self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CommunicationEntity':
        """Create from dictionary"""
        # Handle timestamp conversion
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = None
        
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
            source_node=data.get('source_node', ''),
            target_node=data.get('target_node', ''),
            target_address=data.get('target_address', ''),
            communication_type=data.get('communication_type', ''),
            exchange_action=data.get('exchange_action', ''),
            state_uuid=data.get('state_uuid', ''),
            source_index=data.get('source_index', ''),
            source_name=data.get('source_name', ''),
            message=data.get('message', ''),
            status=data.get('status', '')
        )


@dataclass
class WarningEntity(Entity):
    """
    Represents warning events from the cluster
    """
    
    # Entity classification
    entity_type: EntityType = field(default=EntityType.WARNING, init=False)
    
    # Log level (Note, Warning, Error, etc.)
    level: str = "Warning"
    
    # Warning details
    warning_type: str = ""
    warning_message: str = ""
    warning_code: str = ""
    severity: str = "warning"
    
    # Connection-related warnings
    connection_id: str = ""
    thread_id: str = ""
    database_name: str = ""
    user_name: str = ""
    client_host: str = ""
    abort_reason: str = ""
    
    # Source information
    component: str = ""
    subsystem: str = ""
    
    def validate(self) -> bool:
        """Validate warning entity"""
        if not self.warning_message:
            self.warning_message = self.raw_line
        
        if not self.warning_type:
            # Infer from pattern or content
            if "aborted connection" in self.pattern_name.lower() or "aborted connection" in self.warning_message.lower():
                self.warning_type = "connection_abort"
            elif "timeout" in self.warning_message.lower():
                self.warning_type = "timeout"
            else:
                self.warning_type = "general"
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'warning_type': self.warning_type,
            'warning_message': self.warning_message,
            'warning_code': self.warning_code,
            'severity': self.severity,
            'connection_id': self.connection_id,
            'thread_id': self.thread_id,
            'database_name': self.database_name,
            'user_name': self.user_name,
            'client_host': self.client_host,
            'abort_reason': self.abort_reason,
            'component': self.component,
            'subsystem': self.subsystem
        })
        return base_dict

    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for warning entity ID generation"""
        return {
            'type': self.warning_type or 'unknown',
            'severity': self.severity or 'warning',
            'timestamp': self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WarningEntity':
        """Create from dictionary"""
        # Handle timestamp conversion
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = None
        
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
            level=data.get('level', 'Warning'),
            warning_type=data.get('warning_type', ''),
            warning_message=data.get('warning_message', ''),
            warning_code=data.get('warning_code', ''),
            severity=data.get('severity', 'warning'),
            connection_id=data.get('connection_id', ''),
            thread_id=data.get('thread_id', ''),
            database_name=data.get('database_name', ''),
            user_name=data.get('user_name', ''),
            client_host=data.get('client_host', ''),
            abort_reason=data.get('abort_reason', ''),
            component=data.get('component', ''),
            subsystem=data.get('subsystem', '')
        )


@dataclass
class ErrorEntity(Entity):
    """
    Represents error events from the cluster
    """
    
    # Entity classification
    entity_type: EntityType = field(default=EntityType.ERROR, init=False)
    
    # Log level (Note, Warning, Error, etc.)
    level: str = "Error"
    
    # Error details
    error_type: str = ""
    error_message: str = ""
    error_code: str = ""
    severity: str = "error"
    
    # Error context
    component: str = ""
    subsystem: str = ""
    operation: str = ""
    
    # Recovery information
    recovery_action: str = ""
    recovery_success: bool = False
    
    def validate(self) -> bool:
        """Validate error entity"""
        if not self.error_message:
            self.error_message = self.raw_line
        
        if not self.error_type:
            # Infer from pattern or content
            if "timeout" in self.error_message.lower():
                self.error_type = "timeout"
            elif "connection" in self.error_message.lower():
                self.error_type = "connection"
            elif "authentication" in self.error_message.lower():
                self.error_type = "authentication"
            elif "permission" in self.error_message.lower():
                self.error_type = "permission"
            else:
                self.error_type = "general"
        
        return True
    
    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for error entity ID generation"""
        # Try to extract node info from error message or use generic
        node_info = getattr(self, 'node_name', None) or getattr(self, 'node_address', None) or "unknown"
        
        return {
            'timestamp': self.timestamp,
            'node': node_info,
            'error_type': self.error_type or 'general',
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'error_code': self.error_code,
            'severity': self.severity,
            'component': self.component,
            'subsystem': self.subsystem,
            'operation': self.operation,
            'recovery_action': self.recovery_action,
            'recovery_success': self.recovery_success
        })
        return base_dict
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ErrorEntity':
        """Create from dictionary"""
        # Handle timestamp conversion
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = None
        
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
            level=data.get('level', 'Error'),
            error_type=data.get('error_type', ''),
            error_message=data.get('error_message', ''),
            error_code=data.get('error_code', ''),
            severity=data.get('severity', 'error'),
            component=data.get('component', ''),
            subsystem=data.get('subsystem', ''),
            operation=data.get('operation', ''),
            recovery_action=data.get('recovery_action', ''),
            recovery_success=data.get('recovery_success', False)
        )


@dataclass
class PerformanceEntity(Entity):
    """
    Represents performance metrics and monitoring events
    """
    
    # Entity classification
    entity_type: EntityType = field(default=EntityType.PERFORMANCE, init=False)
    
    # Log level
    level: str = "Note"
    
    # Performance metrics
    metric_name: str = ""
    metric_value: Optional[float] = None
    metric_unit: str = ""
    
    # Timing information
    operation_duration: Optional[float] = None  # milliseconds
    operation_name: str = ""
    
    # Resource utilization
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_io: Optional[float] = None
    network_io: Optional[float] = None
    
    # Galera-specific performance
    apply_lag: Optional[float] = None
    commit_lag: Optional[float] = None
    local_queue_size: Optional[int] = None
    recv_queue_size: Optional[int] = None
    
    # Thresholds and alerts
    threshold_exceeded: bool = False
    alert_level: str = "normal"  # normal, warning, critical
    
    def validate(self) -> bool:
        """Validate performance entity"""
        if not self.metric_name and not self.operation_name:
            # Try to infer from raw line
            if "apply" in self.raw_line.lower():
                self.operation_name = "apply"
            elif "commit" in self.raw_line.lower():
                self.operation_name = "commit"
            elif "queue" in self.raw_line.lower():
                self.metric_name = "queue_size"
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'metric_name': self.metric_name,
            'metric_value': self.metric_value,
            'metric_unit': self.metric_unit,
            'operation_duration': self.operation_duration,
            'operation_name': self.operation_name,
            'cpu_usage': self.cpu_usage,
            'memory_usage': self.memory_usage,
            'disk_io': self.disk_io,
            'network_io': self.network_io,
            'apply_lag': self.apply_lag,
            'commit_lag': self.commit_lag,
            'local_queue_size': self.local_queue_size,
            'recv_queue_size': self.recv_queue_size,
            'threshold_exceeded': self.threshold_exceeded,
            'alert_level': self.alert_level
        })
        return base_dict

    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for performance entity ID generation"""
        return {
            'metric': self.metric_name or 'unknown',
            'operation': self.operation_name or 'unknown',
            'timestamp': self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PerformanceEntity':
        """Create from dictionary"""
        # Handle timestamp conversion
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = None
        
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
            level=data.get('level', 'Note'),
            metric_name=data.get('metric_name', ''),
            metric_value=data.get('metric_value'),
            metric_unit=data.get('metric_unit', ''),
            operation_duration=data.get('operation_duration'),
            operation_name=data.get('operation_name', ''),
            cpu_usage=data.get('cpu_usage'),
            memory_usage=data.get('memory_usage'),
            disk_io=data.get('disk_io'),
            network_io=data.get('network_io'),
            apply_lag=data.get('apply_lag'),
            commit_lag=data.get('commit_lag'),
            local_queue_size=data.get('local_queue_size'),
            recv_queue_size=data.get('recv_queue_size'),
            threshold_exceeded=data.get('threshold_exceeded', False),
            alert_level=data.get('alert_level', 'normal')
        )


@dataclass
class TransactionEntity(Event):
    """
    Represents transaction-related events and operations
    """
    
    # Entity classification
    entity_type: EntityType = field(default=EntityType.TRANSACTION, init=False)
    
    # Log level
    level: str = "Note"
    
    # Transaction identification
    transaction_id: str = ""
    global_transaction_id: str = ""
    thread_id: str = ""
    
    # Transaction details
    transaction_type: str = ""  # commit, rollback, start, deadlock
    transaction_state: str = ""  # active, committed, aborted
    isolation_level: str = ""
    
    # Galera-specific transaction info
    seqno: Optional[int] = None
    depends_seqno: Optional[int] = None
    certification_outcome: str = ""  # pass, fail
    
    # Performance metrics
    transaction_duration: Optional[float] = None  # milliseconds
    rows_affected: Optional[int] = None
    lock_wait_time: Optional[float] = None
    
    # Conflict detection
    has_conflict: bool = False
    conflict_type: str = ""  # certification, deadlock, timeout
    conflicting_transaction: str = ""
    
    def validate(self) -> bool:
        """Validate transaction entity"""
        # Call parent validation
        super().validate()
        
        if not self.transaction_type:
            # Infer from raw line content
            raw_lower = self.raw_line.lower()
            if "commit" in raw_lower:
                self.transaction_type = "commit"
            elif "rollback" in raw_lower:
                self.transaction_type = "rollback"
            elif "deadlock" in raw_lower:
                self.transaction_type = "deadlock"
                self.has_conflict = True
                self.conflict_type = "deadlock"
            elif "certification" in raw_lower:
                self.transaction_type = "certification"
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'transaction_id': self.transaction_id,
            'global_transaction_id': self.global_transaction_id,
            'thread_id': self.thread_id,
            'transaction_type': self.transaction_type,
            'transaction_state': self.transaction_state,
            'isolation_level': self.isolation_level,
            'seqno': self.seqno,
            'depends_seqno': self.depends_seqno,
            'certification_outcome': self.certification_outcome,
            'transaction_duration': self.transaction_duration,
            'rows_affected': self.rows_affected,
            'lock_wait_time': self.lock_wait_time,
            'has_conflict': self.has_conflict,
            'conflict_type': self.conflict_type,
            'conflicting_transaction': self.conflicting_transaction
        })
        return base_dict

    def get_id_attributes(self) -> Dict[str, Any]:
        """Get attributes for transaction entity ID generation"""
        transaction_id = self.transaction_id or self.global_transaction_id or str(self.seqno) if self.seqno else 'unknown'
        return {
            'id': transaction_id,
            'type': self.transaction_type or 'unknown',
            'timestamp': self.timestamp,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransactionEntity':
        """Create from dictionary"""
        # Handle timestamp conversion
        timestamp = data.get('timestamp')
        if timestamp and isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                timestamp = None
        
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
            level=data.get('level', 'Note'),
            transaction_id=data.get('transaction_id', ''),
            global_transaction_id=data.get('global_transaction_id', ''),
            thread_id=data.get('thread_id', ''),
            transaction_type=data.get('transaction_type', ''),
            transaction_state=data.get('transaction_state', ''),
            isolation_level=data.get('isolation_level', ''),
            seqno=data.get('seqno'),
            depends_seqno=data.get('depends_seqno'),
            certification_outcome=data.get('certification_outcome', ''),
            transaction_duration=data.get('transaction_duration'),
            rows_affected=data.get('rows_affected'),
            lock_wait_time=data.get('lock_wait_time'),
            has_conflict=data.get('has_conflict', False),
            conflict_type=data.get('conflict_type', ''),
            conflicting_transaction=data.get('conflicting_transaction', '')
        )


# Register entity classes with the registry (to be imported by other modules)
def register_core_entities(registry):
    """
    Register core entity classes with an EntityRegistry
    
    Args:
        registry: EntityRegistry instance to register with
    """
    # Import enhanced node entities
    try:
        from .enhanced_nodes import ClusterEntity, NodeEntity as EnhancedNodeEntity, NodeStateEntity
        # Register enhanced entities
        registry.register_entity_class(EntityType.CLUSTER, ClusterEntity)
        registry.register_entity_class(EntityType.NODE, EnhancedNodeEntity)
        registry.register_entity_class(EntityType.NODE_STATE, NodeStateEntity)
    except ImportError as e:
        # Fallback to legacy NODE entity if enhanced entities not available
        print(f"WARNING: Enhanced entities not available: {e}")
        registry.register_entity_class(EntityType.NODE, NodeEntity)
    
    # Register other core entities
    registry.register_entity_class(EntityType.STATE_TRANSFER, StateTransferEntity)
    registry.register_entity_class(EntityType.VIEW, ViewEntity)
    registry.register_entity_class(EntityType.WSREP_VIEW, WsrepViewEntity)
    registry.register_entity_class(EntityType.COMMUNICATION, CommunicationEntity)
    registry.register_entity_class(EntityType.WARNING, WarningEntity)
    registry.register_entity_class(EntityType.ERROR, ErrorEntity)
    registry.register_entity_class(EntityType.PERFORMANCE, PerformanceEntity)
    registry.register_entity_class(EntityType.TRANSACTION, TransactionEntity)