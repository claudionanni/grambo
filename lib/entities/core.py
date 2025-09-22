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
    node_id: str = ""  # Galera node UUID
    node_name: str = ""  # Human-readable node name
    node_address: str = ""  # IP:port combination
    
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
        if not self.node_id and not self.node_name and not self.node_address:
            raise ValueError("Node must have at least one identifier (id, name, or address)")
            
        # Validate state transition if both states are present
        if self.previous_state and self.current_state:
            valid_transitions = self._get_valid_transitions()
            if (self.previous_state, self.current_state) not in valid_transitions:
                # Log warning but don't fail validation (state might be incomplete)
                self.validation_notes += f"Unusual state transition: {self.previous_state.value} -> {self.current_state.value}"
                
        return True
        
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
            transition = f"{self.previous_state.value} -> {new_state.value}"
            if self.validation_notes:
                self.validation_notes += f"; State transition: {transition}"
            else:
                self.validation_notes = f"State transition: {transition}"
                
    def to_dict(self) -> Dict[str, Any]:
        """Convert node entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'node_id': self.node_id,
            'node_name': self.node_name,
            'node_address': self.node_address,
            'current_state': self.current_state.value,
            'previous_state': self.previous_state.value if self.previous_state else None,
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
            node_id=data.get('node_id', ''),
            node_name=data.get('node_name', ''),
            node_address=data.get('node_address', ''),
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
                
    def to_dict(self) -> Dict[str, Any]:
        """Convert state transfer entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'transfer_type': self.transfer_type.value,
            'transfer_method': self.transfer_method.value if self.transfer_method else None,
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


# Register entity classes with the registry (to be imported by other modules)
def register_core_entities(registry):
    """
    Register core entity classes with an EntityRegistry
    
    Args:
        registry: EntityRegistry instance to register with
    """
    registry.register_entity_class(EntityType.NODE, NodeEntity)
    registry.register_entity_class(EntityType.STATE_TRANSFER, StateTransferEntity)
    registry.register_entity_class(EntityType.VIEW, ViewEntity)
    registry.register_entity_class(EntityType.COMMUNICATION, CommunicationEntity)
    registry.register_entity_class(EntityType.WARNING, WarningEntity)