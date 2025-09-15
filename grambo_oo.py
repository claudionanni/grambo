#!/usr/bin/env python3
"""
Grambo OO - Galera Log Deforester (Object-Oriented Edition)
Analyze MySQL/MariaDB Galera cluster log files with full object modeling.

Usage:
    python grambo_oo.py <log_file>
    cat <log_file> | python grambo_oo.py
    python grambo_oo.py --format=json <log_file>
"""

import sys
import re
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, asdict, field
from enum import Enum

class EventType(Enum):
    SERVER_INFO = "server_info"
    CLUSTER_VIEW = "cluster_view"
    STATE_TRANSITION = "state_transition"
    SST_EVENT = "sst_event"
    IST_EVENT = "ist_event"
    COMMUNICATION_ISSUE = "communication_issue"
    WARNING = "warning"
    ERROR = "error"

@dataclass
class Node:
    """Represents a Galera cluster node"""
    uuid: str
    name: str
    node_id: Optional[str] = None  # Format: "0.1", "1.1", etc (segment.index)
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
        """Find node by UUID (supports both full and short UUIDs)"""
        # First try exact match
        if uuid in self.nodes:
            return self.nodes[uuid]
        
        # If not found and this looks like a short UUID, try to find matching full UUID
        if len(uuid) < 36 and '-' in uuid:  # Short UUID format like 378c0ec7-a3db
            for full_uuid, node in self.nodes.items():
                if full_uuid.startswith(uuid.split('-')[0]) and uuid in full_uuid:
                    return node
        
        return None
    
    def update_view(self, view: 'ClusterView') -> None:
        """Update cluster with new view"""
        self.current_view = view
        self.views_history.append(view)
    
    @property
    def node_count(self) -> int:
        return len(self.nodes)
    
    @property
    def active_nodes(self) -> List[Node]:
        """Return nodes that are currently active/synced"""
        return [node for node in self.nodes.values() 
                if node.status and node.status.upper() in ['SYNCED', 'DONOR']]

@dataclass 
class ClusterView:
    """Cluster view/membership change event"""
    view_id: str
    status: str
    timestamp: str
    protocol_version: Optional[str] = None
    capabilities: List[str] = field(default_factory=list)
    members: Dict[str, Node] = field(default_factory=dict)
    joined: List[Node] = field(default_factory=list)
    left: List[Node] = field(default_factory=list)
    partitioned: List[Node] = field(default_factory=list)
    
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
        return self.to_state.upper() == 'DONOR'
    
    @property
    def is_joining(self) -> bool:
        return self.to_state.upper() in ['JOINED', 'JOINER']
    
    @property
    def is_syncing(self) -> bool:
        return self.to_state.upper() == 'SYNCED'
    
    @property
    def is_leaving(self) -> bool:
        return self.from_state.upper() in ['SYNCED', 'DONOR'] and self.to_state.upper() in ['OPEN', 'CLOSED']

@dataclass
class LogEvent:
    """Represents a parsed log event with full context"""
    event_type: EventType
    timestamp: str
    raw_message: str
    node: Optional[Node] = None
    cluster_view: Optional[ClusterView] = None
    state_transition: Optional[StateTransition] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ClusterHealthMetrics:
    """Cluster health metrics and analysis"""
    total_state_transitions: int = 0
    total_view_changes: int = 0
    nodes_that_left: Set[str] = field(default_factory=set)
    nodes_that_joined: Set[str] = field(default_factory=set) 
    sst_events: int = 0
    ist_events: int = 0
    communication_issues: int = 0
    warnings: int = 0
    errors: int = 0
    
    @property
    def stability_score(self) -> float:
        """Calculate cluster stability score (0-100)"""
        base_score = 100.0
        
        # Deduct points for instability indicators
        base_score -= min(self.total_view_changes * 5, 30)  # Max 30 points for view changes
        base_score -= min(self.total_state_transitions * 2, 20)  # Max 20 points for transitions
        base_score -= min(self.communication_issues * 10, 25)  # Max 25 points for comm issues
        base_score -= min(self.errors * 15, 25)  # Max 25 points for errors
        
        return max(base_score, 0.0)
    
    @property
    def health_status(self) -> str:
        """Return human-readable health status"""
        score = self.stability_score
        if score >= 90:
            return "Excellent"
        elif score >= 75:
            return "Good"
        elif score >= 60:
            return "Fair"
        elif score >= 40:
            return "Poor"
        else:
            return "Critical"

class GaleraLogAnalyzer:
    """Object-oriented Galera log analyzer that maintains full cluster state"""
    
    def __init__(self):
        self.cluster = Cluster()
        self.events: List[LogEvent] = []
        self.health_metrics = ClusterHealthMetrics()
        self.current_node_name: Optional[str] = None
        self.current_timestamp: Optional[str] = None
        self.line_number = 0
        
    def parse_log(self, log_lines: List[str]) -> None:
        """Parse log lines and build complete cluster state"""
        for line in log_lines:
            self.line_number += 1
            self._parse_line(line)
    
    def _parse_line(self, line: str) -> None:
        """Parse a single log line and extract all relevant information"""
        # Extract timestamp
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', line)
        if timestamp_match:
            self.current_timestamp = timestamp_match.group(1)
        
        # Extract and track node information
        self._extract_node_info(line)
        
        # Parse different event types in order of specificity
        if self._parse_cluster_view(line):
            return
        if self._parse_state_transition(line):
            return
        if self._parse_sst_event(line):
            return
        if self._parse_ist_event(line):
            return
        if self._parse_communication_issue(line):
            return
        if self._parse_error(line):
            return
        if self._parse_warning(line):
            return
        if self._parse_server_info(line):
            return
    
    def _extract_node_info(self, line: str) -> None:
        """Extract and maintain node information from log line"""
        # Extract UUID and node name pairs from member lists (most accurate)
        uuid_name_match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}),\s*([A-Za-z0-9_-]+)', line)
        if uuid_name_match:
            uuid = uuid_name_match.group(1)
            name = uuid_name_match.group(2)
            if name and not name.upper() in ['INFO', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'TRACE', 'NOTE']:
                self.current_node_name = name
                self._ensure_node_exists_with_uuid(name, uuid)
                return
        
        # Extract just node names from context (less reliable, don't update current_node_name)
        node_patterns = [
            r'members\(\d+\):\s*\d+:\s*[0-9a-f-]+,\s*([A-Za-z0-9_-]+)',  # member lines
        ]
        
        for pattern in node_patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                node_name = match.group(1)
                # Filter out false positives and ensure it looks like a hostname
                if (node_name and 
                    not node_name.upper() in ['INFO', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'TRACE', 'NOTE'] and
                    len(node_name) > 2 and
                    not node_name.isdigit() and
                    not node_name == 'tcp'):  # Exclude 'tcp' which appears in addresses
                    # Don't set current_node_name here, just ensure the node exists
                    self._ensure_node_exists(node_name)
                    break
    
    def _ensure_node_exists(self, node_name: str) -> Node:
        """Ensure a node exists in the cluster, create if necessary"""
        existing_node = self.cluster.get_node_by_name(node_name)
        if not existing_node:
            # Create UUID based on node name
            node_uuid = f"node_{node_name}_{len(self.cluster.nodes)}"
            new_node = Node(uuid=node_uuid, name=node_name)
            self.cluster.add_node(new_node)
            return new_node
        return existing_node
    
    def _ensure_node_exists_with_uuid(self, node_name: str, uuid: str) -> Node:
        """Ensure a node exists with specific UUID"""
        existing_node = self.cluster.get_node_by_uuid(uuid)
        if existing_node:
            # Update name if it's different
            if existing_node.name != node_name:
                existing_node.name = node_name
            return existing_node
        
        # Check if we have this node by name with different UUID
        existing_by_name = self.cluster.get_node_by_name(node_name)
        if existing_by_name:
            # Remove old entry and re-add with new UUID
            old_uuid = existing_by_name.uuid
            if old_uuid in self.cluster.nodes:
                del self.cluster.nodes[old_uuid]
            existing_by_name.uuid = uuid
            self.cluster.nodes[uuid] = existing_by_name
            return existing_by_name
        
        # Create new node
        new_node = Node(uuid=uuid, name=node_name)
        self.cluster.add_node(new_node)
        return new_node
    
    def _get_current_node(self) -> Optional[Node]:
        """Get the current node being processed"""
        if self.current_node_name:
            return self.cluster.get_node_by_name(self.current_node_name)
        return None
    
    def _parse_cluster_view(self, line: str) -> bool:
        """Parse cluster view/membership changes"""
        # Look for view definitions with detailed membership
        view_match = re.search(r'view\(view_id\((\w+),([^,]+),(\d+)\)', line)
        if view_match and self.current_timestamp:
            status = view_match.group(1).lower()  # PRIM or NON_PRIM
            view_uuid = view_match.group(2)
            view_seq = view_match.group(3)
            view_id = f"{view_uuid}.{view_seq}"
            
            view = ClusterView(
                view_id=view_id,
                status='primary' if status == 'prim' else 'non-primary',
                timestamp=self.current_timestamp
            )
            
            # Store for member parsing that follows
            self.cluster.update_view(view)
            self.health_metrics.total_view_changes += 1
            
            event = LogEvent(
                event_type=EventType.CLUSTER_VIEW,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                cluster_view=view,
                node=self._get_current_node()
            )
            self.events.append(event)
            return True
        
        # Parse member entries in memb {}, joined {}, left {}, partitioned {} sections
        if self.cluster.current_view:
            # Members in the memb {} section
            member_match = re.search(r'^\s*([0-9a-f-]+),(\d+)\s*$', line.strip())
            if member_match:
                uuid = member_match.group(1)
                
                # Try to find existing node first
                node = self.cluster.get_node_by_uuid(uuid)
                if node:
                    # Add existing node to current view members
                    self.cluster.current_view.members[uuid] = node
                    return True
                else:
                    # For membership tracking, we need to record all UUIDs mentioned
                    # but we should avoid creating permanent nodes for connection-only UUIDs
                    if len(uuid) >= 36:  # Full UUID, likely a real node
                        temp_name = f"Node-{uuid[:8]}"
                        node = Node(uuid=uuid, name=temp_name)
                        self.cluster.add_node(node)
                        self.cluster.current_view.members[uuid] = node
                        return True
                    else:
                        # Short UUID - create a temporary node for view tracking only
                        # Don't add to the permanent cluster.nodes collection
                        temp_node = Node(uuid=uuid, name=f"Temp-{uuid[:8]}")
                        self.cluster.current_view.members[uuid] = temp_node
                        return True
            
            # Check for section headers
            if re.match(r'^\s*} (joined|left|partitioned) {', line):
                # These indicate section changes - we'll parse the members that follow
                return True
        
        return False
    
    def _parse_state_transition(self, line: str) -> bool:
        """Parse state transition events"""
        # Look for explicit node state changes (these are cluster states like prim/non_prim)
        cluster_state_match = re.search(r'Node\s+([0-9a-f-]+)\s+state\s+(\w+)', line, re.IGNORECASE)
        if cluster_state_match:
            node_uuid = cluster_state_match.group(1)
            cluster_state = cluster_state_match.group(2)  # prim, non_prim
            
            # Try to find existing node by UUID (handles short UUID matching)
            node = self.cluster.get_node_by_uuid(node_uuid)
            if not node:
                # Check if this is a short UUID that should not create a new node
                # Short UUIDs like "378c0ec7-a3db" should not create separate nodes
                if len(node_uuid) < 36:
                    # Don't create nodes for short UUIDs, they should match existing ones
                    return False
                    
                # Create node with full UUID only
                temp_name = f"Node-{node_uuid[:8]}"
                node = Node(uuid=node_uuid, name=temp_name)
                self.cluster.add_node(node)
            
            if self.current_timestamp:
                transition = StateTransition(
                    node=node,
                    from_state="unknown",
                    to_state=cluster_state,
                    timestamp=self.current_timestamp,
                    reason="Cluster state transition"
                )
                
                self.health_metrics.total_state_transitions += 1
                
                event = LogEvent(
                    event_type=EventType.STATE_TRANSITION,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    state_transition=transition,
                    node=node
                )
                self.events.append(event)
                return True
        
        # Look for WSREP node state transitions (SYNCED, DONOR/DESYNCED, JOINED, etc.)
        # "Shifting SYNCED -> DONOR/DESYNCED (TO: 1625)"
        shifting_match = re.search(r'Shifting\s+([A-Z/]+)\s*->\s*([A-Z/]+)\s*\(TO:\s*(\d+)\)', line, re.IGNORECASE)
        if shifting_match:
            current_node = self._get_current_node()
            if current_node and self.current_timestamp:
                from_state = shifting_match.group(1)
                to_state = shifting_match.group(2)
                sequence_number = shifting_match.group(3)
                
                # Update node status
                current_node.status = to_state
                
                transition = StateTransition(
                    node=current_node,
                    from_state=from_state,
                    to_state=to_state,
                    timestamp=self.current_timestamp,
                    sequence_number=sequence_number,
                    reason="WSREP state transition"
                )
                
                self.health_metrics.total_state_transitions += 1
                
                event = LogEvent(
                    event_type=EventType.STATE_TRANSITION,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    state_transition=transition,
                    node=current_node
                )
                self.events.append(event)
                return True
        
        # Look for other WSREP state patterns  
        other_wsrep_patterns = [
            r'State\s+transfer\s+.*?(\w+)\s*->\s*(\w+)',
            r'changing\s+state\s+(\w+)\s*->\s*(\w+)',
        ]
        
        for pattern in other_wsrep_patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                current_node = self._get_current_node()
                if current_node and self.current_timestamp:
                    from_state = match.group(1)
                    to_state = match.group(2)
                    
                    # Update node status
                    current_node.status = to_state
                    
                    transition = StateTransition(
                        node=current_node,
                        from_state=from_state,
                        to_state=to_state,
                        timestamp=self.current_timestamp,
                        reason="WSREP state transition"
                    )
                    
                    self.health_metrics.total_state_transitions += 1
                    
                    event = LogEvent(
                        event_type=EventType.STATE_TRANSITION,
                        timestamp=self.current_timestamp,
                        raw_message=line.strip(),
                        state_transition=transition,
                        node=current_node
                    )
                    self.events.append(event)
                    return True
        return False
    
    def _parse_sst_event(self, line: str) -> bool:
        """Parse SST (State Snapshot Transfer) events with detailed information"""
        
        # Pattern 1: SST Request with donor selection
        # "Member 1.1 (UAT-DB-01) requested state transfer from '*any*'. Selected 0.1 (UAT-DB-03)(SYNCED) as donor."
        request_match = re.search(
            r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+requested\s+state\s+transfer\s+from\s+[\'"]([^\'\"]+)[\'\"]\.\s+Selected\s+(\d+\.\d+)\s+\(([^)]+)\)\(([^)]+)\)\s+as\s+donor',
            line, re.IGNORECASE
        )
        if request_match:
            if self.current_timestamp:
                joiner_id = request_match.group(1)
                joiner_name = request_match.group(2)
                request_from = request_match.group(3)
                donor_id = request_match.group(4)
                donor_name = request_match.group(5)
                donor_status = request_match.group(6)
                
                # Ensure nodes exist and update their node IDs
                joiner_node = self._ensure_node_exists(joiner_name)
                joiner_node.node_id = joiner_id
                donor_node = self._ensure_node_exists(donor_name)
                donor_node.node_id = donor_id
                
                self.health_metrics.sst_events += 1
                
                sst_info = {
                    'subtype': 'sst_requested',
                    'joiner': joiner_name,
                    'joiner_id': joiner_id,
                    'donor': donor_name,
                    'donor_id': donor_id,
                    'donor_status': donor_status,
                    'requested_from': request_from,
                    'status': 'requested'
                }
                
                event = LogEvent(
                    event_type=EventType.SST_EVENT,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=joiner_node,
                    metadata=sst_info
                )
                self.events.append(event)
                return True
        
        # Pattern 2: SST Started on donor
        # "WSREP_SST: [INFO] mariabackup SST started on donor (20250915 13:45:56.276)"
        started_match = re.search(
            r'WSREP_SST:\s+\[INFO\]\s+(\w+)\s+SST\s+started\s+on\s+(donor|joiner)\s+\(([^)]+)\)',
            line, re.IGNORECASE
        )
        if started_match:
            if self.current_timestamp:
                method = started_match.group(1)  # mariabackup, rsync, etc
                role = started_match.group(2)    # donor or joiner
                sst_timestamp = started_match.group(3)
                
                self.health_metrics.sst_events += 1
                
                sst_info = {
                    'subtype': 'sst_started',
                    'method': method,
                    'role': role,
                    'sst_timestamp': sst_timestamp,
                    'status': 'started'
                }
                
                event = LogEvent(
                    event_type=EventType.SST_EVENT,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=self._get_current_node(),
                    metadata=sst_info
                )
                self.events.append(event)
                return True
        
        # Pattern 3: SST Failures
        # "SST sending failed: -32"
        failed_match = re.search(r'SST\s+(sending|receiving)\s+failed:\s*(-?\d+)', line, re.IGNORECASE)
        if failed_match:
            if self.current_timestamp:
                operation = failed_match.group(1)  # sending or receiving
                error_code = failed_match.group(2)
                
                self.health_metrics.sst_events += 1
                
                sst_info = {
                    'subtype': 'sst_failed',
                    'operation': operation,
                    'error_code': error_code,
                    'status': 'failed'
                }
                
                event = LogEvent(
                    event_type=EventType.SST_EVENT,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=self._get_current_node(),
                    metadata=sst_info
                )
                self.events.append(event)
                return True
        
        # Pattern 4: General Member messages with node IDs and status
        # "Member 0.1 (UAT-DB-03) synced with group."
        member_match = re.search(r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+(\w+)', line)
        if member_match:
            node_id = member_match.group(1)
            node_name = member_match.group(2)
            status_action = member_match.group(3)  # synced, left, etc.
            
            # Update node with ID and status
            node = self._ensure_node_exists(node_name)
            node.node_id = node_id
            
            # Set appropriate status based on action
            if status_action.lower() == 'synced':
                node.status = 'SYNCED'
            elif status_action.lower() in ['left', 'disconnected']:
                node.status = 'LEFT'
                
            return True
        
        # Pattern 5: General SST status events
        # "SST completed" or other SST status messages
        status_patterns = [
            (r'SST\s+(completed|success)', 'completed'),
            (r'SST.*?(requested|sending|receiving)', 'in_progress'),
            (r'(wsrep_sst_|WSREP_SST_)', 'general'),
        ]
        
        for pattern, status in status_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                if self.current_timestamp:
                    self.health_metrics.sst_events += 1
                    
                    sst_info = {
                        'subtype': 'sst_status',
                        'status': status,
                        'method': self._extract_sst_type(line)
                    }
                    
                    event = LogEvent(
                        event_type=EventType.SST_EVENT,
                        timestamp=self.current_timestamp,
                        raw_message=line.strip(),
                        node=self._get_current_node(),
                        metadata=sst_info
                    )
                    self.events.append(event)
                    return True
        
        return False
    
    def _parse_ist_event(self, line: str) -> bool:
        """Parse IST (Incremental State Transfer) events"""
        ist_patterns = [
            r'(IST|ist).*?(requested|sending|receiving|completed|failed)',
            r'Incremental\s+state\s+transfer',
        ]
        
        for pattern in ist_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                if self.current_timestamp:
                    self.health_metrics.ist_events += 1
                    
                    event = LogEvent(
                        event_type=EventType.IST_EVENT,
                        timestamp=self.current_timestamp,
                        raw_message=line.strip(),
                        node=self._get_current_node()
                    )
                    self.events.append(event)
                    return True
        return False
    
    def _parse_communication_issue(self, line: str) -> bool:
        """Parse communication and network issues"""
        comm_patterns = [
            r'(timeout|failed to connect|connection.*lost|network.*error)',
            r'(gcomm|Protocol.*failed|handshake.*failed)',
        ]
        
        for pattern in comm_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                if self.current_timestamp:
                    self.health_metrics.communication_issues += 1
                    
                    event = LogEvent(
                        event_type=EventType.COMMUNICATION_ISSUE,
                        timestamp=self.current_timestamp,
                        raw_message=line.strip(),
                        node=self._get_current_node()
                    )
                    self.events.append(event)
                    return True
        return False
    
    def _parse_error(self, line: str) -> bool:
        """Parse error events"""
        if re.search(r'\b(ERROR|FATAL)\b', line, re.IGNORECASE):
            if self.current_timestamp:
                self.health_metrics.errors += 1
                
                event = LogEvent(
                    event_type=EventType.ERROR,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=self._get_current_node()
                )
                self.events.append(event)
                return True
        return False
    
    def _parse_warning(self, line: str) -> bool:
        """Parse warning events"""
        if re.search(r'\b(WARN|WARNING)\b', line, re.IGNORECASE):
            if self.current_timestamp:
                self.health_metrics.warnings += 1
                
                event = LogEvent(
                    event_type=EventType.WARNING,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=self._get_current_node()
                )
                self.events.append(event)
                return True
        return False
    
    def _parse_server_info(self, line: str) -> bool:
        """Parse server information"""
        # STATE_EXCHANGE messages with state transaction UUIDs
        state_exchange_match = re.search(r'STATE_EXCHANGE:\s+(sent|got)\s+state\s+(UUID|msg):\s+([0-9a-f-]+)', line, re.IGNORECASE)
        if state_exchange_match:
            action = state_exchange_match.group(1)  # sent or got
            uuid_type = state_exchange_match.group(2)  # UUID or msg
            state_uuid = state_exchange_match.group(3)  # The actual state transaction UUID
            
            if self.current_timestamp:
                # Parse node info if this is a "got" message with node details
                node_info = ""
                from_match = re.search(r'from\s+(\d+)\s+\(([^)]+)\)', line)
                if from_match:
                    node_id = from_match.group(1)
                    node_name = from_match.group(2)
                    node_info = f"from {node_id} ({node_name})"
                    
                    # Ensure node exists
                    self._ensure_node_exists(node_name)
                
                event = LogEvent(
                    event_type=EventType.SERVER_INFO,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=self._get_current_node(),
                    metadata={
                        'subtype': 'state_exchange',
                        'action': action,
                        'state_uuid': state_uuid,
                        'node_info': node_info
                    }
                )
                self.events.append(event)
                return True
        
        # Server node sync status messages
        # "Server UAT-DB-03 synced with group"
        server_sync_match = re.search(r'Server\s+([A-Za-z0-9_-]+)\s+synced\s+with\s+group', line, re.IGNORECASE)
        if server_sync_match:
            node_name = server_sync_match.group(1)
            
            # Update node status
            node = self._ensure_node_exists(node_name)
            node.status = 'SYNCED'
            
            if self.current_timestamp:
                event = LogEvent(
                    event_type=EventType.SERVER_INFO,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    node=node,
                    metadata={'subtype': 'server_sync', 'node_name': node_name}
                )
                self.events.append(event)
                return True
        
        # Server startup/configuration messages
        info_patterns = [
            r'(version|started|initialized|ready)',
            r'(galera|wsrep).*?(version|enabled)',
        ]
        
        for pattern in info_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                if self.current_timestamp:
                    event = LogEvent(
                        event_type=EventType.SERVER_INFO,
                        timestamp=self.current_timestamp,
                        raw_message=line.strip(),
                        node=self._get_current_node()
                    )
                    self.events.append(event)
                    return True
        return False
    
    def _infer_transition_reason(self, line: str) -> str:
        """Infer the reason for a state transition"""
        if re.search(r'sst', line, re.IGNORECASE):
            return "SST operation"
        elif re.search(r'ist', line, re.IGNORECASE):
            return "IST operation"
        elif re.search(r'join', line, re.IGNORECASE):
            return "Node joining"
        elif re.search(r'leave|disconnect', line, re.IGNORECASE):
            return "Node leaving"
        else:
            return "Normal operation"
    
    def _extract_sst_type(self, line: str) -> str:
        """Extract SST method type"""
        if re.search(r'rsync', line, re.IGNORECASE):
            return "rsync"
        elif re.search(r'xtrabackup', line, re.IGNORECASE):
            return "xtrabackup"
        elif re.search(r'mysqldump', line, re.IGNORECASE):
            return "mysqldump"
        else:
            return "unknown"
    
    def get_cluster_summary(self) -> Dict[str, Any]:
        """Get comprehensive cluster summary"""
        return {
            "cluster_info": {
                "total_nodes": self.cluster.node_count,
                "active_nodes": len(self.cluster.active_nodes),
                "current_view": asdict(self.cluster.current_view) if self.cluster.current_view else None,
                "total_views": len(self.cluster.views_history)
            },
            "nodes": {node.name: asdict(node) for node in self.cluster.nodes.values()},
            "health_metrics": asdict(self.health_metrics),
            "health_summary": {
                "stability_score": self.health_metrics.stability_score,
                "health_status": self.health_metrics.health_status
            },
            "timeline": {
                "total_events": len(self.events),
                "events_by_type": {event_type.value: len([e for e in self.events if e.event_type == event_type]) 
                                 for event_type in EventType}
            }
        }
    
    def get_state_transitions(self) -> List[StateTransition]:
        """Get all state transitions with full context"""
        return [event.state_transition for event in self.events 
                if event.event_type == EventType.STATE_TRANSITION and event.state_transition]
    
    def get_cluster_views(self) -> List[ClusterView]:
        """Get all cluster view changes"""
        return [event.cluster_view for event in self.events 
                if event.event_type == EventType.CLUSTER_VIEW and event.cluster_view]

def output_text(analyzer: GaleraLogAnalyzer) -> str:
    """Generate human-readable text output"""
    lines = []
    lines.append("="*70)
    lines.append("GALERA CLUSTER LOG ANALYSIS (Object-Oriented)")
    lines.append("="*70)
    
    summary = analyzer.get_cluster_summary()
    
    # Cluster Overview
    lines.append("\n📊 CLUSTER OVERVIEW")
    lines.append("-" * 50)
    cluster_info = summary["cluster_info"]
    lines.append(f"Total Nodes Detected: {cluster_info['total_nodes']}")
    lines.append(f"Active Nodes: {cluster_info['active_nodes']}")
    lines.append(f"Total View Changes: {cluster_info['total_views']}")
    
    # Current cluster view - fix the view ID display
    if cluster_info['current_view']:
        view = cluster_info['current_view']
        lines.append(f"Current View: {view['view_id']} ({view['status']})")
        member_count = len(view.get('members', {}))
        lines.append(f"View Members: {member_count}")
    
    # Health Status
    lines.append("\n🏥 CLUSTER HEALTH")
    lines.append("-" * 50)
    health = summary["health_summary"]
    lines.append(f"Health Status: {health['health_status']}")
    lines.append(f"Stability Score: {health['stability_score']:.1f}/100")
    
    metrics = summary["health_metrics"]
    lines.append(f"State Transitions: {metrics['total_state_transitions']}")
    lines.append(f"View Changes: {metrics['total_view_changes']}")
    lines.append(f"SST Events: {metrics['sst_events']}")
    lines.append(f"IST Events: {metrics['ist_events']}")
    lines.append(f"Communication Issues: {metrics['communication_issues']}")
    lines.append(f"Warnings: {metrics['warnings']}")
    lines.append(f"Errors: {metrics['errors']}")
    
    # Node Details - only show real nodes and display full UUIDs
    lines.append("\n🖥️  CLUSTER NODES")
    lines.append("-" * 50)
    real_nodes = {name: info for name, info in summary["nodes"].items() 
                  if not name.isdigit() and len(name) > 2 and 
                  name not in ['Note', 'INFO', 'WARN', 'ERROR', 'tcp'] and
                  not name.startswith('Node-') and
                  not name.startswith('node_')}  # Also filter generated node names
    
    if real_nodes:
        for node_name, node_info in real_nodes.items():
            status = node_info.get('status', 'Unknown')
            uuid = node_info.get('uuid', 'N/A')
            node_id = node_info.get('node_id', None)
            
            node_line = f"• {node_name}: {status}"
            if node_id:
                node_line += f" (Node ID: {node_id})"
            node_line += f" (UUID: {uuid})"
            lines.append(node_line)
    
    # Show additional UUID-only nodes only if they have meaningful information  
    uuid_nodes = {name: info for name, info in summary["nodes"].items()
                  if (name.startswith('Node-') or (len(name) > 10 and '-' in name and name.count('-') >= 2)) and
                  name not in real_nodes and
                  len(name) >= 36}  # Only show full UUIDs, not short ones
    
    if uuid_nodes:
        lines.append("\n   Additional Node UUIDs:")
        for node_name, node_info in uuid_nodes.items():
            status = node_info.get('status', 'Unknown')
            uuid = node_info.get('uuid', 'N/A')
            if uuid != node_name:  # Don't repeat if UUID is the name
                lines.append(f"   • UUID {uuid}: {status}")
    
    if not real_nodes and not uuid_nodes:
        lines.append("• No distinct cluster nodes identified")
    
    # Node State Transition Timelines
    wsrep_transitions = [t for t in analyzer.get_state_transitions() 
                        if t.reason == "WSREP state transition"]
    if wsrep_transitions:
        lines.append("\n🔄 NODE STATE TRANSITION TIMELINES")
        lines.append("-" * 50)
        
        # Group transitions by node
        transitions_by_node = {}
        for transition in wsrep_transitions:
            node_name = transition.node.name
            if node_name not in transitions_by_node:
                transitions_by_node[node_name] = []
            transitions_by_node[node_name].append(transition)
        
        for node_name, transitions in transitions_by_node.items():
            lines.append(f"\n📋 {node_name}:")
            
            # Create timeline format
            timeline_parts = []
            for i, transition in enumerate(transitions):
                timestamp = transition.timestamp[11:19]  # Extract HH:MM:SS
                to_seq = f"(TO:{transition.sequence_number})" if transition.sequence_number else ""
                
                if i == 0:
                    # First transition shows the from_state
                    timeline_parts.append(f"{transition.from_state}")
                
                timeline_parts.append(f" --{timestamp}--> {transition.to_state}{to_seq}")
            
            # Join timeline parts and add to output
            full_timeline = "".join(timeline_parts)
            
            # Break long lines for readability
            if len(full_timeline) > 70:
                # Show as individual transitions
                for transition in transitions:
                    timestamp = transition.timestamp[11:19]
                    to_seq = f" (TO:{transition.sequence_number})" if transition.sequence_number else ""
                    lines.append(f"  {timestamp}: {transition.from_state} → {transition.to_state}{to_seq}")
            else:
                lines.append(f"  {full_timeline}")
    
    # State Transitions with better context and node names
    transitions = analyzer.get_state_transitions()
    if transitions:
        lines.append("\n🔄 ALL STATE TRANSITIONS")
        lines.append("-" * 50)
        for transition in transitions[-10:]:  # Show last 10
            node_display = transition.node.name if hasattr(transition.node, 'name') else str(transition.node.uuid)[:8]
            transition_type = "CLUSTER" if transition.reason == "Cluster state transition" else "WSREP"
            seq_info = f" (TO:{transition.sequence_number})" if transition.sequence_number else ""
            lines.append(f"{transition.timestamp}: [{node_display}] {transition.from_state} → {transition.to_state}{seq_info} ({transition_type})")
            if transition.reason and transition.reason not in ["Cluster state transition", "WSREP state transition"]:
                lines.append(f"   Reason: {transition.reason}")
    
    # Cluster Views
    views = analyzer.get_cluster_views()
    if views:
        lines.append("\n🔍 CLUSTER VIEW CHANGES")
        lines.append("-" * 50)
        for view in views[-5:]:  # Show last 5
            lines.append(f"{view.timestamp}: View {view.view_id} ({view.status})")
            lines.append(f"   Members: {view.member_count}")
    
    # Events by type
    lines.append("\n📈 EVENT SUMMARY")
    lines.append("-" * 50)
    timeline = summary["timeline"]
    for event_type, count in timeline["events_by_type"].items():
        if count > 0:
            lines.append(f"• {event_type.replace('_', ' ').title()}: {count}")
    
    # SST/IST Details with comprehensive donor/joiner information
    sst_events = [e for e in analyzer.events if e.event_type == EventType.SST_EVENT]
    ist_events = [e for e in analyzer.events if e.event_type == EventType.IST_EVENT]
    
    if sst_events or ist_events:
        lines.append("\n💾 STATE TRANSFER EVENTS")
        lines.append("-" * 50)
        
        if sst_events:
            lines.append(f"SST Events ({len(sst_events)}):")
            
            # Group SST events by type for better presentation
            sst_requests = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_requested']
            sst_starts = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_started']
            sst_failures = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_failed']
            
            # Show SST requests with donor selection
            if sst_requests:
                lines.append("  📋 SST Requests:")
                for event in sst_requests[-5:]:  # Show last 5
                    meta = event.metadata
                    joiner = meta.get('joiner', 'Unknown')
                    donor = meta.get('donor', 'Unknown') 
                    donor_status = meta.get('donor_status', '')
                    requested_from = meta.get('requested_from', '*any*')
                    
                    lines.append(f"    {event.timestamp}: {joiner} requested SST from '{requested_from}'")
                    lines.append(f"      → Donor selected: {donor} ({donor_status})")
            
            # Show SST starts
            if sst_starts:
                lines.append("  🚀 SST Started:")
                for event in sst_starts[-5:]:  # Show last 5
                    meta = event.metadata
                    method = meta.get('method', 'unknown')
                    role = meta.get('role', 'unknown')
                    sst_time = meta.get('sst_timestamp', '')
                    node_name = event.node.name if event.node else "Unknown"
                    
                    lines.append(f"    {event.timestamp}: [{node_name}] {method} SST started as {role}")
                    if sst_time:
                        lines.append(f"      SST Timestamp: {sst_time}")
            
            # Show SST failures
            if sst_failures:
                lines.append("  ❌ SST Failures:")
                for event in sst_failures[-5:]:  # Show last 5
                    meta = event.metadata
                    operation = meta.get('operation', 'unknown')
                    error_code = meta.get('error_code', 'unknown')
                    node_name = event.node.name if event.node else "Unknown"
                    
                    lines.append(f"    {event.timestamp}: [{node_name}] SST {operation} failed (error: {error_code})")
            
            # Show other SST events
            other_sst = [e for e in sst_events if not e.metadata or 
                        e.metadata.get('subtype') not in ['sst_requested', 'sst_started', 'sst_failed']]
            if other_sst:
                lines.append("  📊 Other SST Events:")
                for event in other_sst[-3:]:
                    node_name = event.node.name if event.node else "Unknown"
                    meta = event.metadata or {}
                    status = meta.get('status', 'unknown')
                    method = meta.get('method', 'unknown')
                    lines.append(f"    {event.timestamp}: [{node_name}] SST {status} ({method})")
        
        if ist_events:
            lines.append(f"\nIST Events ({len(ist_events)}):")
            for event in ist_events[-3:]:  # Show last 3
                node_name = event.node.name if event.node else "Unknown"
                lines.append(f"  {event.timestamp}: [{node_name}] IST")
    
    # Recent Critical Events
    critical_events = [e for e in analyzer.events 
                      if e.event_type in [EventType.ERROR, EventType.COMMUNICATION_ISSUE]]
    if critical_events:
        lines.append("\n⚠️  CRITICAL EVENTS")
        lines.append("-" * 50)
        for event in critical_events[-5:]:  # Show last 5
            node_name = event.node.name if event.node else "Unknown"
            lines.append(f"{event.timestamp}: [{node_name}] {event.event_type.value.upper()}")
            lines.append(f"   {event.raw_message[:80]}...")
    
    return "\n".join(lines)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Galera Log Analyzer (Object-Oriented)')
    parser.add_argument('logfile', nargs='?', help='Galera log file to analyze')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--filter', help='Filter events by type (comma-separated)')
    
    args = parser.parse_args()
    
    # Read input
    if args.logfile:
        try:
            with open(args.logfile, 'r') as f:
                log_lines = f.readlines()
        except FileNotFoundError:
            print(f"Error: File '{args.logfile}' not found", file=sys.stderr)
            sys.exit(1)
    else:
        log_lines = sys.stdin.readlines()
    
    # Analyze logs
    analyzer = GaleraLogAnalyzer()
    analyzer.parse_log(log_lines)
    
    # Output results
    if args.format == 'json':
        summary = analyzer.get_cluster_summary()
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(output_text(analyzer))

if __name__ == "__main__":
    main()
