"""
Core entity classes for Galera cluster log analysis

This module implements the specific entity types used in Galera cluster analysis:
- NodeEntity: Represents a cluster node and its state
- StateTransferEntity: Represents SST/IST operations
- ViewEntity: Represents cluster membership changes
"""

from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
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


@dataclass(frozen=True)
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
    node_id: str = ""  # Galera node UUID (short format)
    long_uuid: str = ""  # Full UUID format (when available)
    node_name: str = ""  # Human-readable node name
    node_address: str = ""  # IP:port combination
    state_uuid: str = ""  # State exchange UUID (when available)
    
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

    # UUID history (full + aliases) tracked over time
    uuid_history: Tuple[str, ...] = field(default_factory=tuple)
    
    def validate(self) -> bool:
        """Validate node entity data"""
        if not self.node_id and not self.node_name and not self.node_address:
            raise ValueError("Node must have at least one identifier (id, name, or address)")
            
        # Validate state transition if both states are present
        if self.previous_state and self.current_state:
            valid_transitions = self._get_valid_transitions()
            if (self.previous_state, self.current_state) not in valid_transitions:
                # Log warning but don't fail validation (state might be incomplete)
                prev_state_str = self.previous_state.value if hasattr(self.previous_state, 'value') else str(self.previous_state)
                current_state_str = self.current_state.value if hasattr(self.current_state, 'value') else str(self.current_state)
                # Cannot mutate frozen dataclass; optionally log or raise, or ignore
                pass
                
        return True
    
    def convert_long_uuid_to_short(self):
        """Normalize long UUID and populate node_id when available."""
        # Consolidate UUID history (long + aliases)
        raw_candidates: List[str] = []
        if self.long_uuid:
            raw_candidates.append(self.long_uuid)
        raw_candidates.extend(self.uuid_history)

        normalized_history: List[str] = []
        for value in raw_candidates:
            if not value:
                continue
            normalized = str(value).strip().lower()
            if normalized and normalized not in normalized_history:
                normalized_history.append(normalized)

        if normalized_history:
            object.__setattr__(self, 'uuid_history', tuple(normalized_history))
            object.__setattr__(self, 'long_uuid', normalized_history[0])

        if self.node_id:
            return

        anchor_uuid = normalized_history[0] if normalized_history else (self.long_uuid or "")
        if not anchor_uuid:
            return

        parts = anchor_uuid.split('-')
        if len(parts) == 5:
            short_uuid = f"{parts[0]}-{parts[3]}"
            object.__setattr__(self, 'node_id', short_uuid)
        else:
            # Fallback: keep full UUID as identifier if format is unexpected
            object.__setattr__(self, 'node_id', anchor_uuid)
    
    def __post_init__(self):
        """Post-initialization processing"""
        super().__post_init__()
        # Convert long UUID to short format if needed
        self.convert_long_uuid_to_short()
        
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
        Return a new instance with updated state and timestamp.
        """
        from dataclasses import replace
        prev_state = self.current_state
        new_inst = replace(self,
            previous_state=prev_state,
            current_state=new_state,
            timestamp=timestamp if timestamp else self.timestamp
        )
        # Update validation notes (immutably)
        if prev_state and prev_state != new_state:
            prev_state_str = prev_state.value if hasattr(prev_state, 'value') else str(prev_state)
            new_state_str = new_state.value if hasattr(new_state, 'value') else str(new_state)
            transition = f"{prev_state_str} -> {new_state_str}"
            notes = (self.validation_notes + f"; State transition: {transition}") if self.validation_notes else f"State transition: {transition}"
            new_inst = replace(new_inst, validation_notes=notes)
        return new_inst
    
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
                
        """Convert node entity to dictionary"""
        base_dict = super().to_dict()
        base_dict.update({
            'level': self.level,
            'node_id': self.node_id,
            'long_uuid': self.long_uuid,
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
            node_id=data.get('node_id', ''),
            long_uuid=data.get('long_uuid', ''),
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


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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
    component_uuid: str = ""
    
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
            # Validation only: do not mutate or return new instance
            if self.cluster_state != self.state:
                raise ValueError("cluster_state does not match state")
        elif self.state and self.cluster_state == "unknown":
            if self.cluster_state != self.state:
                raise ValueError("cluster_state does not match state")
    
    def validate(self) -> bool:
        """Validate view entity"""
        # Call parent validation
        super().validate()
        
        if not self.view_id and self.view_seq is None:
            raise ValueError("View must have either view_id or view_seq")
            
        # Ensure member lists have same length if both present
        if (self.members and self.member_addresses and 
            len(self.members) != len(self.member_addresses)):
            # Validation only: do not mutate or return new instance
            raise ValueError("Member count mismatch between UUIDs and addresses")
            
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
        view_identifier = self.view_id if self.view_id else (f"seq_{self.view_seq}" if self.view_seq is not None else "unknown")
        return {
            'id': view_identifier,
            'timestamp': self.timestamp,
        }
        
        
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
            component_uuid=data.get('component_uuid', ''),
            cluster_uuid=data.get('cluster_uuid', ''),
            cluster_state=data.get('cluster_state', 'unknown'),
            members=data.get('members', []),
            member_addresses=data.get('member_addresses', []),
            joined_nodes=data.get('joined_nodes', []),
            left_nodes=data.get('left_nodes', []),
            protocol_version=data.get('protocol_version'),
            evs_protocol_version=data.get('evs_protocol_version')
        )


@dataclass(frozen=True)
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
                # Validation only: do not mutate or return new instance
                if self.communication_type != "connection":
                    raise ValueError("communication_type should be 'connection'")
            elif "state_exchange" in self.pattern_name.lower():
                if self.communication_type != "state_exchange":
                    raise ValueError("communication_type should be 'state_exchange'")
            elif "cleanup" in self.pattern_name.lower():
                if self.communication_type != "cleanup":
                    raise ValueError("communication_type should be 'cleanup'")
            elif "stable" in self.pattern_name.lower():
                if self.communication_type != "stability":
                    raise ValueError("communication_type should be 'stability'")
        return True
    

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


@dataclass(frozen=True)
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
            if self.warning_message != self.raw_line:
                raise ValueError("warning_message does not match raw_line")
        
        if not self.warning_type:
            # Infer from pattern or content
            if "aborted connection" in self.pattern_name.lower() or "aborted connection" in self.warning_message.lower():
                if self.warning_type != "connection_abort":
                    raise ValueError("warning_type should be 'connection_abort'")
            elif "timeout" in self.warning_message.lower():
                if self.warning_type != "timeout":
                    raise ValueError("warning_type should be 'timeout'")
            else:
                if self.warning_type != "general":
                    raise ValueError("warning_type should be 'general'")
        
        return True
    

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


@dataclass(frozen=True)
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
            if self.error_message != self.raw_line:
                raise ValueError("error_message does not match raw_line")
        
        if not self.error_type:
            # Infer from pattern or content
            if "timeout" in self.error_message.lower():
                if self.error_type != "timeout":
                    raise ValueError("error_type should be 'timeout'")
            elif "connection" in self.error_message.lower():
                if self.error_type != "connection":
                    raise ValueError("error_type should be 'connection'")
            elif "authentication" in self.error_message.lower():
                if self.error_type != "authentication":
                    raise ValueError("error_type should be 'authentication'")
            elif "permission" in self.error_message.lower():
                if self.error_type != "permission":
                    raise ValueError("error_type should be 'permission'")
            else:
                if self.error_type != "general":
                    raise ValueError("error_type should be 'general'")
        
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


@dataclass(frozen=True)
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
                if self.operation_name != "apply":
                    raise ValueError("operation_name should be 'apply'")
            elif "commit" in self.raw_line.lower():
                if self.operation_name != "commit":
                    raise ValueError("operation_name should be 'commit'")
            elif "queue" in self.raw_line.lower():
                if self.metric_name != "queue_size":
                    raise ValueError("metric_name should be 'queue_size'")
        
        return True
    

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


@dataclass(frozen=True)
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
                if self.transaction_type != "commit":
                    raise ValueError("transaction_type should be 'commit'")
            elif "rollback" in raw_lower:
                if self.transaction_type != "rollback":
                    raise ValueError("transaction_type should be 'rollback'")
            elif "deadlock" in raw_lower:
                if self.transaction_type != "deadlock":
                    raise ValueError("transaction_type should be 'deadlock'")
                if not self.has_conflict:
                    raise ValueError("has_conflict should be True for deadlock")
                if self.conflict_type != "deadlock":
                    raise ValueError("conflict_type should be 'deadlock'")
            elif "certification" in raw_lower:
                if self.transaction_type != "certification":
                    raise ValueError("transaction_type should be 'certification'")
        
        return True
    

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
    registry.register_entity_class(EntityType.NODE, NodeEntity)
    registry.register_entity_class(EntityType.STATE_TRANSFER, StateTransferEntity)
    registry.register_entity_class(EntityType.VIEW, ViewEntity)
    registry.register_entity_class(EntityType.COMMUNICATION, CommunicationEntity)
    registry.register_entity_class(EntityType.WARNING, WarningEntity)
    registry.register_entity_class(EntityType.ERROR, ErrorEntity)
    registry.register_entity_class(EntityType.PERFORMANCE, PerformanceEntity)
    registry.register_entity_class(EntityType.TRANSACTION, TransactionEntity)