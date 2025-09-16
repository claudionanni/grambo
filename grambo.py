#!/usr/bin/env python3
"""
Grambo - Galera Log Deforester (Python Edition)
Analyze MySQL/MariaDB Galera cluster log files with clear, organized output.

Usage:
    python grambo.py <log_file>
    cat <log_file> | python grambo.py
    python grambo.py --format=json <log_file>
"""

import sys
import re
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, NamedTuple, Set
from dataclasses import dataclass, asdict, field
from enum import Enum

@dataclass
class ClusterHealthAnalysis:
    """High-level cluster health analysis"""
    cluster_stability: str  # stable, degraded, failing, split-brain
    node_count_changes: int
    sst_failures: int
    network_partitions: int
    quorum_loss_events: int
    recommended_actions: List[str]
    risk_level: str  # low, medium, high, critical

@dataclass
class StateTransitionAnalysis:
    """Analysis of state transition patterns"""
    frequent_transitions: bool
    stuck_in_donor: bool
    sst_retry_cycles: int
    recovery_time_seconds: Optional[float]
    stability_issues: List[str]

@dataclass
class NetworkHealthAnalysis:
    """Network connectivity and stability analysis"""
    connection_failures: int
    unstable_nodes: List[str]
    frequent_reconnects: int
    network_partition_risk: str  # low, medium, high


class EventType(Enum):
    """Types of Galera events we can parse"""
    SERVER_INFO = "server_info"
    CLUSTER_VIEW = "cluster_view" 
    STATE_TRANSITION = "state_transition"
    MEMBERSHIP = "membership"
    QUORUM_STATUS = "quorum_status"
    SST_EVENT = "sst_event"
    IST_EVENT = "ist_event"
    COMMUNICATION = "communication"
    WARNING = "warning"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class LogEvent:
    """Base class for all parsed log events"""
    timestamp: str
    event_type: EventType
    node_id: Optional[str] = None
    raw_line: str = ""
    details: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


@dataclass
class ServerInfo:
    """Server configuration and startup information"""
    version: Optional[str] = None
    socket: Optional[str] = None  
    port: Optional[str] = None
    wsrep_version: Optional[str] = None
    node_uuid: Optional[str] = None
    group_uuid: Optional[str] = None
    group_name: Optional[str] = None
    peers: Optional[str] = None
    address: Optional[str] = None
    galera_version: Optional[str] = None


@dataclass
class Node:
    """Represents a Galera cluster node"""
    uuid: str
    name: str
    address: Optional[str] = None
    index: Optional[int] = None
    status: Optional[str] = None  # SYNCED, DONOR, JOINED, JOINER
    
    def __hash__(self):
        return hash(self.uuid)
    
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.uuid == other.uuid
        return False

@dataclass
class Cluster:
    """Represents the entire Galera cluster state"""
    name: Optional[str] = None
    uuid: Optional[str] = None
    nodes: Dict[str, Node] = field(default_factory=dict)
    current_view: Optional['ClusterView'] = None
    views_history: List['ClusterView'] = field(default_factory=list)
    
    def add_node(self, node: Node) -> None:
        """Add a node to the cluster"""
        self.nodes[node.uuid] = node
    
    def get_node_by_name(self, name: str) -> Optional[Node]:
        """Find node by name"""
        return next((node for node in self.nodes.values() if node.name == name), None)
    
    def get_node_by_uuid(self, uuid: str) -> Optional[Node]:
        """Find node by UUID"""
        return self.nodes.get(uuid)
    
    def update_view(self, view: 'ClusterView') -> None:
        """Update cluster with new view"""
        self.current_view = view
        self.views_history.append(view)

@dataclass 
class ClusterView:
    """Cluster view/membership change event"""
    view_id: str
    status: str
    protocol_version: Optional[str] = None
    capabilities: List[str] = field(default_factory=list)
    members: Dict[str, Node] = field(default_factory=dict)
    joined: List[Node] = field(default_factory=list)
    left: List[Node] = field(default_factory=list)
    partitioned: List[Node] = field(default_factory=list)
    timestamp: Optional[str] = None
    
    @property
    def member_count(self) -> int:
        return len(self.members)
    
    @property
    def is_primary(self) -> bool:
        return self.status == 'primary'
    
    @property
    def is_non_primary(self) -> bool:
        return self.status != 'primary'

@dataclass
class StateTransition:
    """Node state transition event"""
    node: Node
    from_state: str
    to_state: str
    timestamp: str
    sequence_number: Optional[str] = None
    reason: Optional[str] = None
    view_id: Optional[str] = None
    
    @property
    def is_becoming_donor(self) -> bool:
        return self.to_state.lower() == 'donor'
    
    @property
    def is_joining(self) -> bool:
        return self.to_state.lower() in ['joined', 'joiner']
    
    @property
    def is_syncing(self) -> bool:
        return self.to_state.lower() == 'synced'


@dataclass
class MembershipEvent:
    """Cluster membership change event"""
    event_type: str  # joined, left, partitioned, requested_sst, selected_donor
    node_uuid: Optional[str] = None
    node_name: Optional[str] = None
    donor_uuid: Optional[str] = None
    donor_name: Optional[str] = None
    details: Optional[str] = None


@dataclass
class QuorumStatus:
    """Quorum and cluster status information"""
    version: Optional[str] = None
    component: Optional[str] = None
    conf_id: Optional[str] = None
    members_joined: Optional[str] = None
    members_total: Optional[str] = None
    act_id: Optional[str] = None
    last_appl: Optional[str] = None
    protocols: Optional[str] = None
    vote_policy: Optional[str] = None
    group_uuid: Optional[str] = None


@dataclass
class SSTEvent:
    """State Snapshot Transfer event"""
    event_subtype: str  # request, start, complete, failed
    donor: Optional[str] = None
    joiner: Optional[str] = None
    method: Optional[str] = None


class GaleraLogAnalyzer:
    """Object-oriented Galera log analyzer that maintains cluster state"""
    
    def __init__(self):
        self.cluster = Cluster()
        self.events: List[LogEvent] = []
        self.current_node_name: Optional[str] = None
        self.current_timestamp: Optional[str] = None
        
    def parse_log(self, log_lines: List[str]) -> None:
        """Parse log lines and build cluster state"""
        for line in log_lines:
            self._parse_line(line)
    
    def _parse_line(self, line: str) -> None:
        """Parse a single log line"""
        # Extract timestamp and node name if present
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if timestamp_match:
            self.current_timestamp = timestamp_match.group(1)
        
        # Extract node name from various patterns
        node_patterns = [
            r'\[(\w+)\]',  # [nodename]
            r'Node\s+(\w+)',  # Node nodename
            r'on\s+(\w+)',  # on nodename
        ]
        
        for pattern in node_patterns:
            match = re.search(pattern, line)
            if match:
                node_name = match.group(1)
                if node_name and node_name not in ['INFO', 'WARN', 'ERROR']:
                    self.current_node_name = node_name
                    # Create or update node
                    existing_node = self.cluster.get_node_by_name(node_name)
                    if not existing_node:
                        # Create a temporary UUID for nodes without explicit UUIDs
                        temp_uuid = f"node_{node_name}_{len(self.cluster.nodes)}"
                        new_node = Node(uuid=temp_uuid, name=node_name)
                        self.cluster.add_node(new_node)
                break
        
        # Parse different event types
        self._parse_server_info(line)
        self._parse_cluster_view(line)
        self._parse_state_transition(line)
        self._parse_sst_event(line)
        self._parse_ist_event(line)
        self._parse_communication_issue(line)
        self._parse_warning(line)
        self._parse_error(line)
    
    def _get_current_node(self) -> Optional[Node]:
        """Get the current node being processed"""
        if self.current_node_name:
            return self.cluster.get_node_by_name(self.current_node_name)
        return None
    
    def _parse_state_transition(self, line: str) -> None:
        """Parse state transition events with proper Node objects"""
        patterns = [
            (r'Shifting\s+.+\s+(\w+)\s*->\s*(\w+).*\((\d+)\)', 'state_transition'),
            (r'Server\s+status:\s+(\w+)\s*->\s*(\w+)', 'server_status'),
        ]
        
        for pattern, event_type in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                current_node = self._get_current_node()
                if current_node and self.current_timestamp:
                    from_state = match.group(1)
                    to_state = match.group(2)
                    sequence_number = match.group(3) if len(match.groups()) >= 3 else None
                    
                    # Update node status
                    current_node.status = to_state
                    
                    # Create state transition
                    transition = StateTransition(
                        node=current_node,
                        from_state=from_state,
                        to_state=to_state,
                        timestamp=self.current_timestamp,
                        sequence_number=sequence_number,
                        reason="Server status change" if event_type == 'server_status' else None
                    )
                    
                    event = LogEvent(
                        event_type=EventType.STATE_TRANSITION,
                        timestamp=self.current_timestamp,
                        message=line.strip(),
                        details=asdict(transition)
                    )
                    self.events.append(event)
                break
    """Main parser for Galera log files"""
    
    def __init__(self):
        self.events: List[LogEvent] = []
        self.server_info = ServerInfo()
        self.node_address = None
        self.node_name = None  # Store the actual node name
        
        # Analysis tracking
        self.cluster_views: List[LogEvent] = []
        self.state_transitions: List[LogEvent] = []
        self.sst_events: List[LogEvent] = []
        self.network_events: List[LogEvent] = []
        self.error_events: List[LogEvent] = []
        
        # Compile regex patterns for performance
        self._compile_patterns()
    
    def _compile_patterns(self):
        """Compile all regex patterns used for parsing"""
        
        # Timestamp pattern - handles various formats
        self.timestamp_pattern = re.compile(
            r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})'
        )
        
        # Server version info
        self.version_pattern = re.compile(
            r'^Version:\s+\'([^\']+)\'.*socket:\s+\'([^\']+)\'.*port:\s+(\d+)'
        )
        
        # WSREP patterns
        self.wsrep_patterns = {
            'node_uuid': re.compile(r'listening at tcp.*\(([^,]+),'),
            'group_uuid': re.compile(r'group uuid:\s+([a-f0-9-]+)'),
            'group_name': re.compile(r'connecting to group\s+([^\s,]+)'),
            'peers': re.compile(r'connecting to group.*,\s*(.+)'),
            'base_host': re.compile(r'base_host\s*=\s*([^;]+)'),
            'base_port': re.compile(r'base_port\s*=\s*([^;]+)'),
            'galera_version': re.compile(r'(.*)\s+by Codership'),
        }
        
        # Enhanced state transition patterns
        self.state_transition_pattern = re.compile(
            r'SHIFTING\s+([A-Z/]+)\s+->\s+([A-Z/]+)(?:\s+\(TO:\s*(\d+)\))?'
        )
        
        # Server status change pattern
        self.server_status_pattern = re.compile(
            r'Server status change\s+([a-z]+)\s+->\s+([a-z]+)'
        )
        
        # Cluster view pattern  
        self.view_pattern = re.compile(
            r'view\(view_id\(([^,]+),([^,]+),(\d+)\)\s+memb\s*\{([^}]*)\}\s*joined\s*\{([^}]*)\}\s*left\s*\{([^}]*)\}\s*partitioned\s*\{([^}]*)\}'
        )
        
        # Enhanced view parsing for member details
        self.view_member_pattern = re.compile(
            r'(\d+):\s*([a-f0-9-]+),\s*([^,\s\n]+)'
        )
        
        # View block detection
        self.view_start_pattern = re.compile(r'View:')
        self.view_id_pattern = re.compile(r'id:\s*([a-f0-9-]+):(\d+)')
        self.view_status_pattern = re.compile(r'status:\s*(\w+)')
        self.view_protocol_pattern = re.compile(r'protocol_version:\s*(\d+)')
        self.view_capabilities_pattern = re.compile(r'capabilities:\s*(.+)')
        self.view_members_count_pattern = re.compile(r'members\((\d+)\):')
        
        # SST request with donor selection pattern (more specific)
        self.sst_donor_selection_pattern = re.compile(
            r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+requested state transfer.*Selected\s+(\d+\.\d+)\s+\(([^)]+)\)\(([^)]+)\)\s+as donor'
        )
        
        # Membership event patterns
        self.membership_patterns = {
            'member_synced': re.compile(r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+synced with group'),
            'sst_request': self.sst_donor_selection_pattern,
            'forgetting_node': re.compile(r'forgetting\s+([a-f0-9-]+)\s+\(tcp://([^)]+)\)'),
            'node_state': re.compile(r'Node\s+([a-f0-9-]+)\s+state\s+([a-z]+)'),
            'server_synced': re.compile(r'Server\s+([^\s]+)\s+synced with group'),
        }
        
        # Quorum results pattern
        self.quorum_pattern = re.compile(
            r'Quorum results:\s*version\s*=\s*(\d+),\s*component\s*=\s*([^,]+),\s*conf_id\s*=\s*(\d+),\s*members\s*=\s*(\d+)/(\d+)\s*\([^)]+\),\s*act_id\s*=\s*(\d+),\s*last_appl\.\s*=\s*(\d+),\s*protocols\s*=\s*([^,]+),\s*vote policy\s*=\s*(\d+),\s*group UUID\s*=\s*([a-f0-9-]+)'
        )
        
        # SST patterns - improved
        self.sst_patterns = {
            'request': re.compile(r"Running:\s+'wsrep_sst_([^\s']+).*--role\s+'([^']+)'.*--address\s+'([^']+)'"),
            'complete': re.compile(r'State transfer to.*complete'),
            'failed': re.compile(r'State transfer to.*failed'),
            'sst_failed_reason': re.compile(r'State transfer to\s+([^\s]+)\s+\(([^)]+)\)\s+failed:\s*(.+)'),
            'sst_sending_failed': re.compile(r'SST sending failed:\s*(.+)'),
        }
        
        # IST pattern
        self.ist_pattern = re.compile(r'(?:ist|incremental state transfer)', re.IGNORECASE)
        
        # Communication patterns
        self.comm_patterns = {
            'suspecting': re.compile(r'suspecting node:\s*([a-f0-9-]+)'),
            'inactive': re.compile(r'marked with nil.*Declaring inactive'),
            'connection_established': re.compile(r'connection established to\s+([a-f0-9-]+)\s+tcp://([^)]+)'),
            'declaring_stable': re.compile(r'declaring\s+([a-f0-9-]+)\s+at\s+tcp://([^\s]+)\s+stable'),
        }

    def parse_file(self, file_path: str) -> None:
        """Parse a log file"""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                self._parse_lines(f)
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading file: {e}")
            sys.exit(1)
    
    def parse_stdin(self) -> None:
        """Parse log data from stdin"""
        self._parse_lines(sys.stdin)
    
    def _parse_lines(self, lines) -> None:
        """Parse log lines from any iterable"""
        current_view_block = []
        in_view_block = False
        
        for line in lines:
            line = line.strip()
            if not line:
                if in_view_block and current_view_block:
                    # Empty line might end view block
                    self._parse_view_block('\n'.join(current_view_block))
                    in_view_block = False
                    current_view_block = []
                continue
            
            # Extract node address if not found yet
            if not self.node_address:
                self.node_address = self._extract_node_address(line)
            
            # Extract node name if not found yet 
            if not self.node_name:
                self.node_name = self._extract_node_name(line)
                
            # Handle multi-line view blocks
            if self.view_start_pattern.search(line):
                in_view_block = True
                current_view_block = [line]
                continue
            elif in_view_block:
                # Check if this line is still part of the view block
                if (line.startswith('View:') or 
                    line.startswith('id:') or
                    line.startswith('status:') or
                    line.startswith('protocol_version:') or
                    line.startswith('capabilities:') or
                    line.startswith('final:') or
                    line.startswith('own_index:') or
                    line.startswith('members(') or 
                    line.startswith('joined(') or 
                    line.startswith('left(') or 
                    line.startswith('partitioned(') or
                    line.startswith(('\t', ' ')) or  # Indented lines
                    re.match(r'^\s*\d+:', line) or  # Member lines like "0: uuid, name"
                    line.startswith('=')):  # Separator lines
                    current_view_block.append(line)
                    # Check for end marker
                    if line.startswith('=') and len(line) > 10:
                        self._parse_view_block('\n'.join(current_view_block))
                        in_view_block = False
                        current_view_block = []
                else:
                    # This line is not part of view block, process accumulated block
                    if current_view_block:
                        self._parse_view_block('\n'.join(current_view_block))
                    in_view_block = False
                    current_view_block = []
                    # Process current line as normal
                    self._parse_line(line)
                continue
            
            # Parse individual lines
            self._parse_line(line)
    
    def _parse_line(self, line: str) -> None:
        """Parse a single log line"""
        timestamp = self._extract_timestamp(line)
        
        # Try to identify and parse different event types
        if self._parse_server_info(line, timestamp):
            return
        elif self._parse_state_transition(line, timestamp):
            return
        elif self._parse_membership_event(line, timestamp):
            return
        elif self._parse_quorum_status(line, timestamp):
            return  
        elif self._parse_sst_event(line, timestamp):
            return
        elif self._parse_ist_event(line, timestamp):
            return
        elif self._parse_communication_event(line, timestamp):
            return
        elif self._parse_warning(line, timestamp):
            return
        elif self._parse_error(line, timestamp):
            return
    
    def _extract_node_address(self, line: str) -> str:
        """Extract node address from log line"""
        # Look for patterns like "tcp://192.168.1.100:4567" or similar
        addr_match = re.search(r'tcp://([^:\s]+:\d+)', line)
        if addr_match:
            return addr_match.group(1)
        return ""
    
    def _extract_node_name(self, line: str) -> str:
        """Extract node name from log line, especially from the first line showing the hostname"""
    # Look for patterns like "ubuntu@node-03:~$" at the beginning of logs
        if not self.node_name:
            hostname_match = re.search(r'@([^:~$\s]+)', line)
            if hostname_match:
                return hostname_match.group(1)
        return ""
    
    def _extract_timestamp(self, line: str) -> str:
        """Extract timestamp from log line"""
        match = self.timestamp_pattern.search(line)
        return match.group(1) if match else ""
    
    def _parse_server_info(self, line: str, timestamp: str) -> bool:
        """Parse server version and configuration info"""
        # Version info
        if match := self.version_pattern.search(line):
            self.server_info.version = match.group(1)
            self.server_info.socket = match.group(2) 
            self.server_info.port = match.group(3)
            return True
            
        # WSREP info
        for info_type, pattern in self.wsrep_patterns.items():
            if match := pattern.search(line):
                setattr(self.server_info, info_type, match.group(1))
                if info_type == 'base_host':
                    self.node_address = match.group(1)
                return True
        
        return False
    
    def _parse_state_transition(self, line: str, timestamp: str) -> bool:
        """Parse state transition events"""
        # Enhanced SHIFTING pattern
        if match := self.state_transition_pattern.search(line):
            reason = None
            if "donor" in line.lower():
                reason = "Acting as donor for SST"
            elif "sst" in line.lower():
                reason = "SST operation"
            
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.STATE_TRANSITION,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(StateTransition(
                    from_state=match.group(1),
                    to_state=match.group(2), 
                    sequence_number=match.group(3),
                    reason=reason
                ))
            )
            self.events.append(event)
            return True
        
        # Server status change pattern
        if match := self.server_status_pattern.search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.STATE_TRANSITION,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(StateTransition(
                    from_state=match.group(1),
                    to_state=match.group(2),
                    reason="Server status change"
                ))
            )
            self.events.append(event)
            return True
        
        return False
    
    def _parse_membership_event(self, line: str, timestamp: str) -> bool:
        """Parse membership change events"""
        # Member synced with group
        if match := self.membership_patterns['member_synced'].search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.MEMBERSHIP,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(MembershipEvent(
                    event_type="synced",
                    node_name=match.group(2),
                    details=f"Member {match.group(1)} synced with group"
                ))
            )
            self.events.append(event)
            return True
        
        # SST request and donor selection
        if match := self.membership_patterns['sst_request'].search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.MEMBERSHIP,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(MembershipEvent(
                    event_type="sst_request",
                    node_name=match.group(2),
                    donor_name=match.group(4),
                    details=f"Member {match.group(1)} requested SST, donor {match.group(3)} selected"
                ))
            )
            self.events.append(event)
            return True
        
        # Forgetting node
        if match := self.membership_patterns['forgetting_node'].search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.MEMBERSHIP,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(MembershipEvent(
                    event_type="forgetting",
                    node_uuid=match.group(1),
                    details=f"Forgetting node {match.group(1)} at {match.group(2)}"
                ))
            )
            self.events.append(event)
            return True
        
        # Node state
        if match := self.membership_patterns['node_state'].search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.MEMBERSHIP,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(MembershipEvent(
                    event_type="state_update",
                    node_uuid=match.group(1),
                    details=f"Node state: {match.group(2)}"
                ))
            )
            self.events.append(event)
            return True
        
        return False
    
    def _parse_quorum_status(self, line: str, timestamp: str) -> bool:
        """Parse quorum status information"""
        if match := self.quorum_pattern.search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.QUORUM_STATUS,
                node_id=self.node_address,
                raw_line=line,
                details=asdict(QuorumStatus(
                    version=match.group(1),
                    component=match.group(2),
                    conf_id=match.group(3),
                    members_joined=match.group(4),
                    members_total=match.group(5),
                    act_id=match.group(6),
                    last_appl=match.group(7),
                    protocols=match.group(8),
                    vote_policy=match.group(9),
                    group_uuid=match.group(10)
                ))
            )
            self.events.append(event)
            return True
        return False
    
    def _parse_view_block(self, view_block: str) -> None:
        """Parse multi-line cluster view block"""
        # Find the timestamp from the line before "View:"
        lines = view_block.split('\n')
        timestamp = ""
        
        # Look for timestamp in the lines before "View:"
        for i, line in enumerate(lines):
            if 'View:' in line and i > 0:
                # Check previous lines for timestamp
                for j in range(i-1, -1, -1):
                    ts = self._extract_timestamp(lines[j])
                    if ts:
                        timestamp = ts
                        break
                break
        
        if not timestamp:
            timestamp = self._extract_timestamp(lines[0])
        
        # Parse view details
        view_id = ""
        status = ""
        protocol_version = ""
        capabilities = []
        members = {}
        member_count = 0
        
        for line in lines:
            # Extract view ID
            if match := self.view_id_pattern.search(line):
                view_id = f"{match.group(1)}:{match.group(2)}"
            
            # Extract status
            elif match := self.view_status_pattern.search(line):
                status = match.group(1)
            
            # Extract protocol version
            elif match := self.view_protocol_pattern.search(line):
                protocol_version = match.group(1)
            
            # Extract capabilities
            elif match := self.view_capabilities_pattern.search(line):
                caps_text = match.group(1).strip()
                capabilities = [c.strip() for c in caps_text.split(',')]
            
            # Extract member count
            elif match := self.view_members_count_pattern.search(line):
                member_count = int(match.group(1))
            
            # Extract member details
            elif match := self.view_member_pattern.search(line):
                index = match.group(1)
                uuid = match.group(2)
                name = match.group(3)
                members[index] = {'uuid': uuid, 'name': name}
        
        if view_id:  # Only create event if we found a view ID
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.CLUSTER_VIEW,
                node_id=self.node_address,
                raw_line=view_block,
                details={
                    'view_id': view_id,
                    'status': status,
                    'protocol_version': protocol_version,
                    'capabilities': capabilities,
                    'member_count': member_count,
                    'members': members,
                    'joined': [],  # These would need to be detected from context
                    'left': [],
                    'partitioned': []
                }
            )
            self.events.append(event)
            
            # Try to determine our node name from the members if not already found
            if not self.node_name and members:
                # Look for members with the current node's UUID or address
                for idx, member_info in members.items():
                    # For now, if we only have one member in the view and it's primary,
                    # it's likely our node (this is a simplification)
                    if member_count == 1 and status == 'primary':
                        self.node_name = member_info.get('name', '')
                        break
    
    def _parse_sst_event(self, line: str, timestamp: str) -> bool:
        """Parse SST (State Snapshot Transfer) events"""
        # Check for detailed SST failure first
        if match := self.sst_patterns['sst_failed_reason'].search(line):
            details = {
                'event_subtype': 'failed_detailed',
                'target_node': match.group(1),
                'target_name': match.group(2),
                'failure_reason': match.group(3)
            }
            
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.SST_EVENT,
                node_id=self.node_address,
                raw_line=line,
                details=details
            )
            self.events.append(event)
            return True
        
        # Check other SST patterns
        for sst_type, pattern in self.sst_patterns.items():
            if sst_type == 'sst_failed_reason':  # Already handled above
                continue
                
            if match := pattern.search(line):
                details = {'event_subtype': sst_type}
                
                if sst_type == 'request' and len(match.groups()) >= 3:
                    details.update({
                        'method': match.group(1),
                        'donor': match.group(2),
                        'joiner': match.group(3)
                    })
                
                event = LogEvent(
                    timestamp=timestamp,
                    event_type=EventType.SST_EVENT,
                    node_id=self.node_address,
                    raw_line=line,
                    details=details
                )
                self.events.append(event)
                return True
        return False
    
    def _parse_ist_event(self, line: str, timestamp: str) -> bool:
        """Parse IST (Incremental State Transfer) events"""
        if self.ist_pattern.search(line):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.IST_EVENT,
                node_id=self.node_address,
                raw_line=line
            )
            self.events.append(event)
            return True
        return False
    
    def _parse_communication_event(self, line: str, timestamp: str) -> bool:
        """Parse communication issues (node suspicions, etc.)"""
        for comm_type, pattern in self.comm_patterns.items():
            if match := pattern.search(line):
                details = {'comm_type': comm_type}
                
                if comm_type == 'suspecting' and len(match.groups()) >= 1:
                    details['suspected_node'] = match.group(1)
                elif comm_type == 'connection_established' and len(match.groups()) >= 2:
                    details['node_uuid'] = match.group(1)
                    details['address'] = match.group(2)
                elif comm_type == 'declaring_stable' and len(match.groups()) >= 2:
                    details['node_uuid'] = match.group(1)
                    details['address'] = match.group(2)
                
                event = LogEvent(
                    timestamp=timestamp,
                    event_type=EventType.COMMUNICATION,
                    node_id=self.node_address,
                    raw_line=line,
                    details=details
                )
                self.events.append(event)
                return True
        return False
    
    def _parse_warning(self, line: str, timestamp: str) -> bool:
        """Parse warning messages"""
        if re.search(r'\[WARNING\]|warning', line, re.IGNORECASE):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.WARNING,
                node_id=self.node_address,
                raw_line=line
            )
            self.events.append(event) 
            return True
        return False
    
    def _parse_error(self, line: str, timestamp: str) -> bool:
        """Parse error messages"""
        if re.search(r'\[ERROR\]|error', line, re.IGNORECASE):
            event = LogEvent(
                timestamp=timestamp,
                event_type=EventType.ERROR,
                node_id=self.node_address,
                raw_line=line
            )
            self.events.append(event)
            return True
        return False


def analyze_cluster_health(parser: 'GaleraLogParser') -> ClusterHealthAnalysis:
    """Analyze overall cluster health based on parsed events"""
    # Categorize events for analysis
    sst_events = [e for e in parser.events if e.event_type == EventType.SST_EVENT]
    state_transitions = [e for e in parser.events if e.event_type == EventType.STATE_TRANSITION]
    cluster_views = [e for e in parser.events if e.event_type == EventType.CLUSTER_VIEW]
    
    # Count critical events
    sst_failures = len([e for e in sst_events if 'failed' in e.raw_line.lower() or 'error' in e.raw_line.lower()])
    state_transition_count = len(state_transitions)
    view_changes = len(cluster_views)
    
    # Analyze node count stability
    node_counts = []
    for view in cluster_views:
        if view.details and 'member_count' in view.details:
            node_counts.append(view.details['member_count'])
    
    node_count_changes = len(set(node_counts)) - 1 if node_counts else 0
    
    # Determine cluster stability
    stability = "stable"
    risk_level = "low"
    recommended_actions = []
    
    if sst_failures > 2:
        stability = "failing"
        risk_level = "critical"
        recommended_actions.extend([
            "Investigate SST failures - check network connectivity",
            "Verify disk space and permissions on donor and joiner nodes",
            "Review SST method configuration (mariabackup vs rsync)"
        ])
    elif state_transition_count > 10:
        stability = "degraded"
        risk_level = "medium"
        recommended_actions.append("Monitor frequent state transitions - may indicate network instability")
    elif node_count_changes > 3:
        stability = "degraded" 
        risk_level = "medium"
        recommended_actions.append("Investigate frequent node membership changes")
    
    # Check for split-brain indicators
    single_node_views = [v for v in cluster_views if v.details and v.details.get('member_count') == 1]
    if len(single_node_views) > 1:
        stability = "split-brain"
        risk_level = "critical"
        recommended_actions.append("CRITICAL: Potential split-brain scenario detected - investigate immediately")
    
    return ClusterHealthAnalysis(
        cluster_stability=stability,
        node_count_changes=node_count_changes,
        sst_failures=sst_failures,
        network_partitions=0,
        quorum_loss_events=len(single_node_views),
        recommended_actions=recommended_actions,
        risk_level=risk_level
    )

def get_cluster_insights(parser: 'GaleraLogParser') -> Dict[str, str]:
    """Generate high-level cluster insights based on WSREP state machine knowledge"""
    insights = {}
    
    # Get categorized events
    state_transitions = [e for e in parser.events if e.event_type == EventType.STATE_TRANSITION]
    cluster_views = [e for e in parser.events if e.event_type == EventType.CLUSTER_VIEW]
    sst_events = [e for e in parser.events if e.event_type == EventType.SST_EVENT]
    
    # WSREP State Machine Insights (from wsrep_api.h analysis)
    synced_to_donor = [t for t in state_transitions if t.details and 'synced' in t.details.get('from_state', '') and 'donor' in t.details.get('to_state', '')]
    if synced_to_donor:
        insights['sst_pattern'] = f"Node became DONOR {len(synced_to_donor)} times - indicates SST requests from joining nodes"
    
    donor_to_joined = [t for t in state_transitions if t.details and 'donor' in t.details.get('from_state', '') and 'joined' in t.details.get('to_state', '')]
    if donor_to_joined:
        insights['sst_completion'] = f"DONOR→JOINED transitions ({len(donor_to_joined)}) indicate SST sending completed"
    
    joined_to_synced = [t for t in state_transitions if t.details and 'joined' in t.details.get('from_state', '') and 'synced' in t.details.get('to_state', '')]
    if joined_to_synced:
        insights['sync_completion'] = f"JOINED→SYNCED transitions ({len(joined_to_synced)}) indicate full cluster synchronization"
    
    # Cluster View Analysis
    if cluster_views:
        member_counts = [v.details.get('member_count', 0) for v in cluster_views if v.details]
        if member_counts:
            min_members = min(member_counts)
            max_members = max(member_counts)
            if min_members == 1 and max_members > 1:
                insights['cluster_fragmentation'] = f"Cluster membership varied from {min_members} to {max_members} nodes - indicates instability"
            elif min_members == max_members and max_members > 1:
                insights['cluster_stability'] = f"Stable cluster with consistent {max_members} nodes"
    
    # SST Analysis
    sst_failures = [e for e in sst_events if 'error' in e.raw_line.lower() or 'failed' in e.raw_line.lower()]
    sst_successes = [e for e in sst_events if 'complete' in e.raw_line.lower() or 'success' in e.raw_line.lower()]
    
    if len(sst_failures) > 0:
        insights['sst_reliability'] = f"SST reliability concern: {len(sst_failures)} failures detected"
    elif len(sst_successes) > 0:
        insights['sst_reliability'] = f"SST operations successful: {len(sst_successes)} successful transfers"
    
    # Network instability patterns
    connection_events = [e for e in parser.events if e.event_type == EventType.COMMUNICATION]
    if len(connection_events) > 10:
        insights['network_activity'] = f"High network activity: {len(connection_events)} connection events may indicate instability"
    
    return insights

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Grambo - Galera Log Deforester (Python Edition)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python grambo.py error.log
  cat error.log | python grambo.py
  python grambo.py --format=json error.log
  python grambo.py --filter=sst,state error.log
        """
    )
    
    parser.add_argument('logfile', nargs='?', help='Galera log file to analyze')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--filter', help='Filter events by type (comma-separated)')
    
    args = parser.parse_args()
    
    # Initialize parser (delegate to OO analyzer for richer output)
    try:
        from gramboo import GaleraLogAnalyzer as OOAnalyzer, output_text as oo_output_text
        use_oo = True
    except Exception:
        use_oo = False
    
    if use_oo:
        analyzer = OOAnalyzer()
        # Parse input
        if args.logfile:
            try:
                with open(args.logfile, 'r') as f:
                    analyzer.parse_log(f.readlines())
            except FileNotFoundError:
                print(f"Error: File '{args.logfile}' not found", file=sys.stderr)
                sys.exit(1)
        else:
            analyzer.parse_log(sys.stdin.readlines())
        
        # Output
        if args.format == 'json':
            import json
            print(json.dumps(analyzer.get_cluster_summary(), indent=2, default=str))
        else:
            print(oo_output_text(analyzer))
        return
    
    # Fallback to legacy inline parser if OO analyzer is unavailable
    galera_parser = GaleraLogParser()
    
    # Parse input
    if args.logfile:
        galera_parser.parse_file(args.logfile)
    else:
        if sys.stdin.isatty():
            print("Error: No input provided. Use a file or pipe data to stdin.")
            parser.print_help()
            sys.exit(1)
        galera_parser.parse_stdin()
    
    # Apply filters if specified
    events = galera_parser.events
    if args.filter:
        filter_types = {EventType(f.strip()) for f in args.filter.split(',')}
        events = [e for e in events if e.event_type in filter_types]
    
    # Output results
    if args.format == 'json':
        output_json(galera_parser.server_info, events)
    else:
        # Generate comprehensive analysis
        health_analysis = analyze_cluster_health(galera_parser)
        cluster_insights = get_cluster_insights(galera_parser)
        
        output_text(galera_parser.server_info, events, galera_parser, health_analysis, cluster_insights)


def output_text(server_info: ServerInfo, events: List[LogEvent], parser: 'GaleraLogParser', 
                health_analysis: Optional[ClusterHealthAnalysis] = None, 
                cluster_insights: Optional[Dict[str, str]] = None) -> None:
    """Output results in human-readable text format"""
    print("=" * 80)
    print("| G R A M B O - Galera Log Deforester (Python Edition)")
    print("=" * 80)
    print()
    
    # Cluster Health Analysis Section
    if health_analysis:
        print("🏥 CLUSTER HEALTH ANALYSIS")
        print("-" * 50)
        print(f"  Overall Status: {health_analysis.cluster_stability.upper()}")
        print(f"  Risk Level: {health_analysis.risk_level.upper()}")
        print(f"  Node Count Changes: {health_analysis.node_count_changes}")
        print(f"  SST Failures: {health_analysis.sst_failures}")
        print(f"  Quorum Loss Events: {health_analysis.quorum_loss_events}")
        
        if health_analysis.recommended_actions:
            print("  📋 Recommended Actions:")
            for action in health_analysis.recommended_actions:
                print(f"    • {action}")
        print()
    
    # Cluster Insights Section
    if cluster_insights:
        print("🔍 CLUSTER INSIGHTS")
        print("-" * 50)
        for category, insight in cluster_insights.items():
            print(f"  {category.replace('_', ' ').title()}: {insight}")
        print()
    
    # Server Information Section
    print("📊 SERVER INFORMATION")
    print("-" * 50)
    if server_info.version:
        print(f"  Version: {server_info.version}")
    if server_info.socket:
        print(f"  Socket: {server_info.socket}")
    if server_info.port:
        print(f"  Port: {server_info.port}")
    if server_info.address:
        print(f"  Address: {server_info.address}")
    print()
    
    # Galera Information Section  
    print("🔗 GALERA CLUSTER INFORMATION")
    print("-" * 50)
    if server_info.galera_version:
        print(f"  Galera Version: {server_info.galera_version}")
    if server_info.node_uuid:
        print(f"  Node UUID: {server_info.node_uuid}")
    if server_info.group_uuid:
        print(f"  Group UUID: {server_info.group_uuid}")
    if server_info.group_name:
        print(f"  Group Name: {server_info.group_name}")
    if server_info.peers:
        print(f"  Peers: {server_info.peers}")
    print()
    
    # Events by Category
    events_by_type = {}
    for event in events:
        if event.event_type not in events_by_type:
            events_by_type[event.event_type] = []
        events_by_type[event.event_type].append(event)
    
    # State Transitions
    if EventType.STATE_TRANSITION in events_by_type:
        print("🔄 STATE TRANSITIONS")
        print("-" * 50)
        for event in events_by_type[EventType.STATE_TRANSITION]:
            details = event.details
            transition_text = f"{details['from_state']} → {details['to_state']}"
            node_info = parser.node_name or parser.node_address or "Unknown Node"
            print(f"  {event.timestamp} | {node_info}: {transition_text}")
            if details.get('sequence_number'):
                print(f"    └─ Sequence: {details['sequence_number']}")
            if details.get('reason'):
                print(f"    └─ Reason: {details['reason']}")
        print()
    
    # Membership Events
    if EventType.MEMBERSHIP in events_by_type:
        print("👤 MEMBERSHIP CHANGES")
        print("-" * 50)
        for event in events_by_type[EventType.MEMBERSHIP]:
            details = event.details
            event_type = details.get('event_type', 'unknown')
            print(f"  {event.timestamp} | {event_type.upper()}")
            if details.get('node_name'):
                print(f"    └─ Node: {details['node_name']}")
            if details.get('donor_name'):
                print(f"    └─ Donor: {details['donor_name']}")
            if details.get('node_uuid'):
                print(f"    └─ UUID: {details['node_uuid']}")
            if details.get('details'):
                print(f"    └─ {details['details']}")
        print()
    
    # Cluster Views
    if EventType.CLUSTER_VIEW in events_by_type:
        print("👥 CLUSTER MEMBERSHIP VIEWS")
        print("-" * 50)
        for event in events_by_type[EventType.CLUSTER_VIEW]:
            details = event.details
            print(f"  {event.timestamp} | View: {details['view_id']}")
            print(f"    └─ Status: {details['status']}, Members: {details['member_count']}")
            
            # Show detailed member information
            if details.get('member_details'):
                print(f"    └─ Member Details:")
                for idx, member_info in details['member_details'].items():
                    print(f"       {idx}: {member_info['name']} ({member_info['uuid'][:8]}...)")
            
            if details.get('protocol_version'):
                print(f"    └─ Protocol Version: {details['protocol_version']}")
            
            if details.get('capabilities'):
                caps = ', '.join(details['capabilities'][:3])  # Show first 3 capabilities
                if len(details['capabilities']) > 3:
                    caps += f" (+{len(details['capabilities'])-3} more)"
                print(f"    └─ Capabilities: {caps}")
            
            if details['joined']:
                print(f"    └─ Joined: {', '.join(details['joined'])}")
            if details['left']:
                print(f"    └─ Left: {', '.join(details['left'])}")
            if details['partitioned']:
                print(f"    └─ Partitioned: {', '.join(details['partitioned'])}")
        print()
    
    # Quorum Status
    if EventType.QUORUM_STATUS in events_by_type:
        print("⚖️  QUORUM STATUS")
        print("-" * 50)
        for event in events_by_type[EventType.QUORUM_STATUS]:
            details = event.details
            print(f"  {event.timestamp} | Quorum Results")
            print(f"    └─ Version: {details.get('version')}, Component: {details.get('component')}")
            print(f"    └─ Members: {details.get('members_joined')}/{details.get('members_total')} (joined/total)")
            print(f"    └─ Config ID: {details.get('conf_id')}, Activity ID: {details.get('act_id')}")
            print(f"    └─ Last Applied: {details.get('last_appl')}")
            print(f"    └─ Protocols: {details.get('protocols')}")
        print()
    
    # SST Events
    if EventType.SST_EVENT in events_by_type:
        print("💾 STATE SNAPSHOT TRANSFER (SST)")
        print("-" * 50)
        for event in events_by_type[EventType.SST_EVENT]:
            details = event.details
            subtype = details.get('event_subtype', 'unknown')
            
            if subtype == 'failed_detailed':
                print(f"  {event.timestamp} | SST FAILED (Detailed)")
                print(f"    └─ Target: {details.get('target_name')} ({details.get('target_node')})")
                print(f"    └─ Reason: {details.get('failure_reason')}")
            else:
                print(f"  {event.timestamp} | SST {subtype.upper()}")
                if details.get('method'):
                    print(f"    └─ Method: {details['method']}")
                if details.get('donor'):
                    print(f"    └─ Donor: {details['donor']}")
                if details.get('joiner'):
                    print(f"    └─ Joiner: {details['joiner']}")
        print()
    
    # IST Events
    if EventType.IST_EVENT in events_by_type:
        print("📈 INCREMENTAL STATE TRANSFER (IST)")
        print("-" * 50)
        for event in events_by_type[EventType.IST_EVENT]:
            print(f"  {event.timestamp} | {event.raw_line}")
        print()
    
    # Communication Issues
    if EventType.COMMUNICATION in events_by_type:
        print("🌐 COMMUNICATION EVENTS")
        print("-" * 50)
        for event in events_by_type[EventType.COMMUNICATION]:
            details = event.details
            comm_type = details.get('comm_type', 'unknown')
            print(f"  {event.timestamp} | {comm_type.upper().replace('_', ' ')}")
            if details.get('suspected_node'):
                print(f"    └─ Suspected Node: {details['suspected_node']}")
            if details.get('node_uuid'):
                print(f"    └─ Node UUID: {details['node_uuid']}")
            if details.get('address'):
                print(f"    └─ Address: {details['address']}")
        print()
    
    # Warnings
    if EventType.WARNING in events_by_type:
        print("⚡ WARNINGS")
        print("-" * 50)
        for event in events_by_type[EventType.WARNING]:
            print(f"  {event.timestamp} | {event.raw_line}")
        print()
    
    # Errors
    if EventType.ERROR in events_by_type:
        print("🚨 ERRORS")
        print("-" * 50)
        for event in events_by_type[EventType.ERROR]:
            print(f"  {event.timestamp} | {event.raw_line}")
        print()
    
    # Summary
    print("📋 SUMMARY")
    print("-" * 50)
    total_events = len(events)
    print(f"  Total Events Parsed: {total_events}")
    for event_type, event_list in events_by_type.items():
        print(f"  {event_type.value.replace('_', ' ').title()}: {len(event_list)}")
    print()


def output_json(server_info: ServerInfo, events: List[LogEvent]) -> None:
    """Output results in JSON format"""
    result = {
        'server_info': asdict(server_info),
        'events': [asdict(event) for event in events],
        'summary': {
            'total_events': len(events),
            'events_by_type': {}
        }
    }
    
    # Calculate summary
    for event in events:
        event_type_str = event.event_type.value
        if event_type_str not in result['summary']['events_by_type']:
            result['summary']['events_by_type'][event_type_str] = 0
        result['summary']['events_by_type'][event_type_str] += 1
    
    print(json.dumps(result, indent=2, default=str))


if __name__ == '__main__':
    main()
