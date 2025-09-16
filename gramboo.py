#!/usr/bin/env python3
"""
Gramboo - Galera Log Deforester
Analyze MySQL/MariaDB Galera cluster log files with full object modeling.

Usage:
    python gramboo.py <log_file>
    cat <log_file> | python gramboo.py
    python gramboo.py --format=json <log_file>
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
    uuid: Optional[str] = None  # Deprecated: not used; prefer node/group/local fields below
    # Group/view state (cluster-level)
    group_uuid: Optional[str] = None
    group_seqno: Optional[int] = None
    # Local provider state on this node
    local_state_uuid: Optional[str] = None
    local_seqno: Optional[int] = None
    # Node instance UUID ("My UUID") for this node
    node_instance_uuid: Optional[str] = None
    local_node_name: Optional[str] = None
    nodes: Dict[str, Node] = field(default_factory=dict)
    current_view: Optional['ClusterView'] = None
    views_history: List['ClusterView'] = field(default_factory=list)
    # Group change timeline (old->new)
    group_changes: List[Dict[str, Any]] = field(default_factory=list)
    
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
    """Galera log analyzer that maintains full cluster state"""
    
    def __init__(self, dialect: str = 'auto', report_unknown: bool = False,
                 mariadb_version: Optional[str] = None,
                 mariadb_edition: Optional[str] = None,
                 galera_version: Optional[str] = None):
        self.cluster = Cluster()
        self.events: List[LogEvent] = []
        self.health_metrics = ClusterHealthMetrics()
        self.current_node_name: Optional[str] = None
        self.current_timestamp: Optional[str] = None
        self.line_number = 0
        self.dialect = dialect  # 'auto' | 'galera-26' | 'mariadb-10' | 'pxc-8' | 'unknown'
        self.report_unknown = report_unknown
        self._unknown_lines: List[str] = []
        # Software/version info discovered while parsing
        self.software: Dict[str, Optional[str]] = {
            'mariadb_version': None,          # e.g., 10.6.16 or 11.4.7-4
            'mariadb_edition': None,          # 'enterprise' | 'community'
            'wsrep_provider_version': None,   # e.g., 26.4.23
            'wsrep_provider_path': None,      # full path to libgalera*.so if observed
            'galera_variant': None,           # 'enterprise' | 'community'
            'galera_vendor': None,            # e.g., 'Codership Oy'
            'galera_version_inferred': None,  # bool-like flag as string 'true' when inferred
        }
        # Apply CLI overrides if provided
        if mariadb_version:
            self.software['mariadb_version'] = mariadb_version
        if mariadb_edition:
            self.software['mariadb_edition'] = mariadb_edition
        if galera_version:
            self.software['wsrep_provider_version'] = galera_version
        # Precompiled IST-related patterns (from ist.cpp)
        self._ist_patterns = {
            'recv_addr': re.compile(r'IST\s+receiver\s+addr\s+using\s+(\S+)', re.IGNORECASE),
            'recv_bind': re.compile(r'IST\s+receiver\s+bind\s+using\s+(\S+)', re.IGNORECASE),
            'receiver_prepared': re.compile(r'Prepared\s+IST\s+receiver\s+for\s+(-?\d+)\s*-\s*(-?\d+),\s+listening\s+at:\s+(\S+)', re.IGNORECASE),
            'applying_start': re.compile(r'IST\s+applying\s+starts\s+with\s+(-?\d+)', re.IGNORECASE),
            'current_initialized': re.compile(r'IST\s+current\s+seqno\s+initialized\s+to\s+(-?\d+)', re.IGNORECASE),
            'preload_start': re.compile(r'IST\s+preload\s+starting\s+at\s+(-?\d+)', re.IGNORECASE),
            'eof': re.compile(r'eof\s+received,\s+closing\s+socket', re.IGNORECASE),
            'incomplete': re.compile(r"IST\s+didn't\s+contain\s+all\s+write\s+sets,\s+expected\s+last:\s*(-?\d+)\s+last\s+received:\s*(-?\d+)", re.IGNORECASE),
            'send_failed': re.compile(r'ist\s+send\s+failed:\s*', re.IGNORECASE),
            'sender_nothing': re.compile(r'IST\s+sender\s+notifying\s+joiner,\s+not\s+sending\s+anything', re.IGNORECASE),
            'sender_range': re.compile(r'IST\s+sender\s+(-?\d+)\s*->\s*(-?\d+)', re.IGNORECASE),
            'async_start': re.compile(r'async\s+IST\s+sender\s+starting\s+to\s+serve\s+(\S+)\s+sending\s+(-?\d+)\s*-\s*(-?\d+),\s+preload\s+starts\s+from\s+(-?\d+)', re.IGNORECASE),
            'async_failed': re.compile(r'async\s+IST\s+sender\s+failed\s+to\s+serve\s+(\S+):\s+(.+)', re.IGNORECASE),
            'async_served': re.compile(r'async\s+IST\s+sender\s+served', re.IGNORECASE),
        }

    def infer_galera_from_mariadb(self) -> None:
        """Infer Galera provider version and variant from MariaDB version/edition when missing.
        This uses a simple heuristic map keyed by MariaDB major.minor series.
        """
        mver = self.software.get('mariadb_version') or ''
        if mver and not self.software.get('wsrep_provider_version'):
            m = re.match(r'^(\d+\.(?:\d+))', mver)
            series = m.group(1) if m else None
            default_map = {
                # Heuristic defaults; can be extended with more series
                '10.6': '26.4.22',
                '11.4': '26.4.23',
            }
            if series and series in default_map:
                self.software['wsrep_provider_version'] = default_map[series]
                self.software['galera_version_inferred'] = 'true'
    # Infer Galera variant (enterprise/community) from MariaDB edition if missing
        if not self.software.get('galera_variant') and self.software.get('mariadb_edition'):
            self.software['galera_variant'] = self.software['mariadb_edition']
        
    def parse_log(self, log_lines: List[str]) -> None:
        """Parse log lines and build complete cluster state"""
        # Auto-detect dialect from the first ~200 lines if needed
        if self.dialect == 'auto':
            self.dialect = self._detect_dialect(log_lines[:200])
        for line in log_lines:
            self.line_number += 1
            self._parse_line(line)

    def _detect_dialect(self, lines: List[str]) -> str:
        text = "\n".join(lines)
        # Simple heuristics; extend as needed
        if re.search(r'PXC|Percona XtraDB Cluster', text, re.IGNORECASE):
            return 'pxc-8'
        if re.search(r'Galera\s+26\.', text, re.IGNORECASE):
            return 'galera-26'
        if re.search(r'Maria(DB)?\s+10\.', text, re.IGNORECASE):
            return 'mariadb-10'
        if re.search(r'WSREP', text):
            return 'galera'
        return 'unknown'
    
    def _parse_line(self, line: str) -> None:
        """Parse a single log line and extract all relevant information"""
        # Extract timestamp
        # Support single or double digit hour and flexible spacing, normalize to HH:MM:SS
        ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})', line)
        if ts_match:
            date_part = ts_match.group(1)
            hour = int(ts_match.group(2))
            minute = ts_match.group(3)
            second = ts_match.group(4)
            self.current_timestamp = f"{date_part} {hour:02d}:{minute}:{second}"
        
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
                # Ensure the node exists with the given UUID/name
                self._ensure_node_exists_with_uuid(name, uuid)
                # If this UUID matches our local instance UUID, capture local node name
                if self.cluster.node_instance_uuid and uuid == self.cluster.node_instance_uuid:
                    self.cluster.local_node_name = name
                # Do NOT set current_node_name from membership lines to avoid misattribution
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
                if (
                    node_name
                    and not node_name.upper() in ['INFO', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'TRACE', 'NOTE']
                    and len(node_name) > 2
                    and not node_name.isdigit()
                    and node_name != 'tcp'  # Exclude 'tcp' which appears in addresses
                ):
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

    def _get_local_node(self) -> Optional[Node]:
        """Resolve the local node (the owner of this log)."""
        # Prefer by UUID if known
        if self.cluster.node_instance_uuid:
            node = self.cluster.get_node_by_uuid(self.cluster.node_instance_uuid)
            if node:
                # If we learned the local name later, keep nodes in sync
                if self.cluster.local_node_name and node.name != self.cluster.local_node_name:
                    node.name = self.cluster.local_node_name
                return node
            # Create a placeholder local node if not present yet
            name = self.cluster.local_node_name or f"Local-{self.cluster.node_instance_uuid[:8]}"
            node = Node(uuid=self.cluster.node_instance_uuid, name=name)
            self.cluster.add_node(node)
            return node
        # Fallback: use local name if known
        if self.cluster.local_node_name:
            return self._ensure_node_exists(self.cluster.local_node_name)
        # Last resort: use current node if set
        return self._get_current_node()
    
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
            # Track group/view state at cluster level
            self.cluster.group_uuid = view_uuid
            try:
                self.cluster.group_seqno = int(view_seq)
            except Exception:
                self.cluster.group_seqno = None
            
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
        # Cluster state lines like: "Node <group_uuid> state PRIM/NON_PRIM" are cluster-level, not node-level
        cluster_state_match = re.search(r'Node\s+([0-9a-f-]+)\s+state\s+(\w+)', line, re.IGNORECASE)
        if cluster_state_match:
            group_or_state_uuid = cluster_state_match.group(1)
            cluster_state = cluster_state_match.group(2)  # prim, non_prim
            # Do not create/alter nodes here; record as server info event and update group/local as appropriate
            if self.current_timestamp:
                # Heuristic: treat the UUID here as group/state UUID
                self.cluster.group_uuid = group_or_state_uuid
                event = LogEvent(
                    event_type=EventType.SERVER_INFO,
                    timestamp=self.current_timestamp,
                    raw_message=line.strip(),
                    metadata={
                        'subtype': 'cluster_state',
                        'group_or_state_uuid': group_or_state_uuid,
                        'state': cluster_state
                    }
                )
                self.events.append(event)
                return True
        
        # gcs.cpp: Restored state X -> Y (SEQNO)
        restored_match = re.search(r'Restored\s+state\s+([A-Z/\-]+)\s*->\s*([A-Z/\-]+)\s*\(([-\d]+)\)', line, re.IGNORECASE)
        if restored_match:
            current_node = self._get_local_node()
            if current_node and self.current_timestamp:
                from_state = restored_match.group(1)
                to_state = restored_match.group(2)
                seqno = restored_match.group(3)

                current_node.status = to_state

                transition = StateTransition(
                    node=current_node,
                    from_state=from_state,
                    to_state=to_state,
                    timestamp=self.current_timestamp,
                    sequence_number=seqno,
                    reason="Restored state"
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
        
        # Look for WSREP node state transitions (SYNCED, DONOR/DESYNCED, JOINED, etc.)
        # "Shifting SYNCED -> DONOR/DESYNCED (TO: 1625)"
        shifting_match = re.search(r'Shifting\s+([A-Z/]+)\s*->\s*([A-Z/]+)\s*\(TO:\s*(\d+)\)', line, re.IGNORECASE)
        if shifting_match:
            # WSREP shifting transitions are local-node events; attribute to local node
            current_node = self._get_local_node()
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
                # Treat WSREP state changes as local unless explicitly qualified otherwise
                current_node = self._get_local_node()
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
        # gcs.cpp: Rejecting State Transfer Request in state 'STATE'. Joiner should be restarted.
        sst_reject = re.search(r"Rejecting\s+State\s+Transfer\s+Request\s+in\s+state\s+'([^']+)'.", line, re.IGNORECASE)
        if sst_reject and self.current_timestamp:
            state = sst_reject.group(1)
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_reject', 'state': state}
            )
            self.health_metrics.sst_events += 1
            self.events.append(event)
            return True

        # gcs.cpp: Received State Transfer Request in wrong state X. Rejecting.
        sst_wrong_state = re.search(r'Received\s+State\s+Transfer\s+Request\s+in\s+wrong\s+state\s+([A-Z/\-]+)\.\s+Rejecting\.', line, re.IGNORECASE)
        if sst_wrong_state and self.current_timestamp:
            bad_state = sst_wrong_state.group(1)
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_reject_wrong_state', 'state': bad_state}
            )
            self.health_metrics.sst_events += 1
            self.events.append(event)
            return True


        # Pattern 1: SST Request with donor selection
        # "Member 1.1 (UAT-DB-01) requested state transfer from '*any*'. Selected 0.1 (UAT-DB-03)(SYNCED) as donor."
        request_match = re.search(
            r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+requested\s+state\s+transfer\s+from\s+[\'\"]([^\'\"]+)[\'\"]\.\s+Selected\s+(\d+\.\d+)\s+\(([^)]+)\)\(([^)]+)\)\s+as\s+donor',
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
        
        # Pattern 2a: Initiating SST/IST transfer with address (donor/joiner side)
        init_match = re.search(
            r"Initiating\s+SST/IST\s+transfer\s+on\s+(DONOR|JOINER)\s+side\s*\((wsrep_sst_[a-z0-9_]+)\s+.*?--role\s+'(donor|joiner)'.*?--address\s+'([^']+)'",
            line, re.IGNORECASE
        )
        if init_match and self.current_timestamp:
            side = init_match.group(1).lower()
            tool = init_match.group(2)
            role = init_match.group(3).lower()
            address = init_match.group(4)
            method = tool.replace('wsrep_sst_', '')
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_initiated', 'role': role, 'side': side, 'method': method, 'address': address}
            )
            self.events.append(event)
            return True

        # Pattern 2b: SST Started on donor/joiner (WSREP_SST helper)
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

        # Pattern 2c: SST Completed on donor/joiner
        completed_match = re.search(
            r"WSREP_SST:\s+\[INFO\]\s+(\w+)\s+SST\s+completed\s+on\s+(donor|joiner)\s+\(([^)]+)\)",
            line, re.IGNORECASE
        )
        if completed_match and self.current_timestamp:
            method = completed_match.group(1)
            role = completed_match.group(2)
            sst_timestamp = completed_match.group(3)
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_completed', 'method': method, 'role': role, 'sst_timestamp': sst_timestamp, 'status': 'completed'}
            )
            self.events.append(event)
            return True

        # Pattern 2d: Provider notice of state transfer complete (to/from)
        complete_notice = re.search(r"State\s+transfer\s+(to|from)\s+(\d+\.\d+)\s+\(([^)]+)\)\s+complete\.", line, re.IGNORECASE)
        if complete_notice and self.current_timestamp:
            direction = complete_notice.group(1).lower()
            peer_id = complete_notice.group(2)
            peer_name = complete_notice.group(3)
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_complete_notice', 'direction': direction, 'peer_id': peer_id, 'peer_name': peer_name}
            )
            self.events.append(event)
            return True

        # Pattern 2e: Streaming the backup to joiner at <addr>
        stream_addr = re.search(r"WSREP_SST:\s+\[INFO\]\s+Streaming\s+the\s+backup\s+to\s+joiner\s+at\s+(\S+)", line, re.IGNORECASE)
        if stream_addr and self.current_timestamp:
            addr = stream_addr.group(1)
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_stream_addr', 'joiner_addr': addr}
            )
            self.events.append(event)
            return True

        # Pattern 2f: SST sent uuid:seqno
        sst_sent = re.search(r"SST\s+sent:\s+([0-9a-f-]+):(\-?\d+)", line, re.IGNORECASE)
        if sst_sent and self.current_timestamp:
            uuid = sst_sent.group(1)
            seqno = int(sst_sent.group(2))
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_sent', 'uuid': uuid, 'seqno': seqno}
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
        
        # Pattern 5a: Total time on donor
        total_time = re.search(r"WSREP_SST:\s+\[INFO\]\s+Total\s+time\s+on\s+donor:\s+(\d+)\s+seconds", line, re.IGNORECASE)
        if total_time and self.current_timestamp:
            secs = int(total_time.group(1))
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_total_time', 'seconds': secs}
            )
            self.events.append(event)
            return True

        # Pattern 5b: Donor monitor total time
        donor_mon = re.search(r"Donor\s+monitor\s+thread\s+ended\s+with\s+total\s+time\s+(\d+)\s+sec", line, re.IGNORECASE)
        if donor_mon and self.current_timestamp:
            secs = int(donor_mon.group(1))
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'donor_monitor_time', 'seconds': secs}
            )
            self.events.append(event)
            return True

        # Pattern 5c: General SST status events
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
        """Parse IST (Incremental State Transfer) events with details derived from ist.cpp logs"""
        ts = self.current_timestamp
        if not ts:
            return False

        # Receiver-side patterns
        m = self._ist_patterns['recv_addr'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_receiver_addr', 'addr': m.group(1), 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['recv_bind'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_receiver_bind', 'bind': m.group(1), 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['receiver_prepared'].search(line)
        if m:
            first, last, addr = int(m.group(1)), int(m.group(2)), m.group(3)
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_receiver_prepared', 'first_seqno': first, 'last_seqno': last, 'listen_addr': addr, 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['applying_start'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_applying_start', 'first_seqno': int(m.group(1)), 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['current_initialized'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_current_initialized', 'seqno': int(m.group(1)), 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['preload_start'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_preload_start', 'seqno': int(m.group(1)), 'role': 'receiver'}
            ))
            return True

        if self._ist_patterns['eof'].search(line):
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_completed', 'role': 'receiver'}
            ))
            return True

        m = self._ist_patterns['incomplete'].search(line)
        if m:
            self.health_metrics.ist_events += 1
            # Count as error as well
            self.health_metrics.errors += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_incomplete', 'expected_last': int(m.group(1)), 'last_received': int(m.group(2)), 'role': 'receiver'}
            ))
            return True

        # Sender-side patterns
        if self._ist_patterns['send_failed'].search(line):
            self.health_metrics.ist_events += 1
            self.health_metrics.errors += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_send_failed', 'role': 'sender'}
            ))
            return True

        if self._ist_patterns['sender_nothing'].search(line):
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_sender_nothing', 'role': 'sender'}
            ))
            return True

        m = self._ist_patterns['sender_range'].search(line)
        if m:
            first, last = int(m.group(1)), int(m.group(2))
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_sender_range', 'first_seqno': first, 'last_seqno': last, 'role': 'sender'}
            ))
            return True

        m = self._ist_patterns['async_start'].search(line)
        if m:
            peer, first, last, preload = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'async_ist_start', 'peer': peer, 'first_seqno': first, 'last_seqno': last, 'preload_start': preload, 'role': 'sender'}
            ))
            return True

        m = self._ist_patterns['async_failed'].search(line)
        if m:
            peer, reason = m.group(1), m.group(2)
            self.health_metrics.ist_events += 1
            self.health_metrics.errors += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'async_ist_failed', 'peer': peer, 'reason': reason, 'role': 'sender'}
            ))
            return True

        if self._ist_patterns['async_served'].search(line):
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'async_ist_served', 'role': 'sender'}
            ))
            return True

        # Fallback generic IST detection
        if re.search(r'(?:\bIST\b|incremental\s+state\s+transfer)', line, re.IGNORECASE):
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_current_node(),
                metadata={'subtype': 'ist_generic'}
            ))
            return True
        # Track unknown WSREP/IST mentions if reporting enabled
        if self.report_unknown and re.search(r'WSREP|IST', line, re.IGNORECASE):
            self._unknown_lines.append(line.strip())
        return False
    
    def _parse_communication_issue(self, line: str) -> bool:
        """Parse communication and network issues"""
        # Structured: Aborted connection (client info)
        m = re.search(r"Aborted\s+connection\s+(\d+)\s+to\s+db:\s+'([^']*)'\s+user:\s+'([^']*)'\s+host:\s+'([^']*)'", line, re.IGNORECASE)
        if m and self.current_timestamp:
            conn_id = int(m.group(1))
            db = m.group(2) or ''
            user = m.group(3) or ''
            host = m.group(4) or ''
            self.health_metrics.communication_issues += 1
            event = LogEvent(
                event_type=EventType.COMMUNICATION_ISSUE,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_current_node(),
                metadata={
                    'subtype': 'aborted_connection',
                    'connection_id': conn_id,
                    'db': db,
                    'user': user,
                    'host': host
                }
            )
            self.events.append(event)
            return True

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
        # Capture MariaDB version from common patterns
        ver1 = re.search(r'Server\s+version:\s*([^\s]+)', line, re.IGNORECASE)
        if ver1:
            raw = ver1.group(1)
            # Extract numeric prefix if present (e.g., 10.6.16-MariaDB-... -> 10.6.16)
            m = re.match(r'([0-9]+(?:\.[0-9]+){1,3})', raw)
            if m and not self.software.get('mariadb_version'):
                self.software['mariadb_version'] = m.group(1)
            if 'mariadb' in raw.lower() and not self.software.get('mariadb_edition'):
                # assume community unless other evidence of enterprise appears
                self.software['mariadb_edition'] = self.software['mariadb_edition'] or 'community'
            # fallthrough to continue parsing other info on same line

        ver2 = re.search(r"Version:\s*'([^']+)'", line, re.IGNORECASE)
        if ver2:
            raw = ver2.group(1)
            m = re.match(r'([0-9]+(?:\.[0-9]+){1,3})', raw)
            if m and not self.software.get('mariadb_version'):
                self.software['mariadb_version'] = m.group(1)
            if 'mariadb-enterprise' in raw.lower():
                self.software['mariadb_edition'] = 'enterprise'
            elif 'mariadb' in raw.lower() and not self.software.get('mariadb_edition'):
                self.software['mariadb_edition'] = 'community'

        # MariaDB Enterprise path hints (from SST helper lines or mysqld args)
        ent_path = re.search(r'mariadb-enterprise-([0-9][0-9\.-]+)', line, re.IGNORECASE)
        if ent_path:
            if not self.software.get('mariadb_version'):
                ver = ent_path.group(1).rstrip('-.')
                self.software['mariadb_version'] = ver
            self.software['mariadb_edition'] = 'enterprise'

        # wsrep provider path (detect enterprise/community variant)
        prov_path = re.search(r"(--wsrep_provider=|wsrep_provider=)('?)([^\s']+libgalera[^\s']*?\.so)\2", line)
        if prov_path:
            self.software['wsrep_provider_path'] = prov_path.group(3)
            if 'enterprise' in prov_path.group(3).lower():
                self.software['galera_variant'] = 'enterprise'
            else:
                self.software['galera_variant'] = self.software['galera_variant'] or 'community'

        # wsrep provider version (typical Galera provider banner)
        prov_ver = re.search(r'WSREP:\s+Provider:\s+Galera\s+([0-9][0-9\.]+)', line)
        if prov_ver and not self.software.get('wsrep_provider_version'):
            self.software['wsrep_provider_version'] = prov_ver.group(1)

        # Alternative generic version hint (fallback)
        if not self.software.get('wsrep_provider_version'):
            m = re.search(r'wsrep[_\s-]*provider[_\s-]*version[^0-9]*([0-9]+(?:\.[0-9]+)+)', line, re.IGNORECASE)
            if m:
                self.software['wsrep_provider_version'] = m.group(1)

        # wsrep_load() provider banner with vendor
        # Example: wsrep_load(): Galera 26.4.22-1(rb31b549) by Codership Oy
        m = re.search(r'wsrep_load\(\):\s*Galera\s+([0-9][\w\.-]+)(?:\([^)]*\))?\s+by\s+([^\r\n]+)', line, re.IGNORECASE)
        if m:
            self.software['wsrep_provider_version'] = self.software.get('wsrep_provider_version') or m.group(1)
            vendor = m.group(2).strip()
            # Clean trailing punctuation
            vendor = vendor.rstrip('. ')
            self.software['galera_vendor'] = vendor

        # Process first view: <group_uuid> my uuid: <node_uuid>
        first_view_match = re.search(r'Process\s+first\s+view:\s+([0-9a-f-]+)\s+my\s+uuid:\s+([0-9a-f-]+)', line, re.IGNORECASE)
        if first_view_match and self.current_timestamp:
            group_uuid = first_view_match.group(1)
            node_uuid = first_view_match.group(2)
            self.cluster.group_uuid = group_uuid
            self.cluster.node_instance_uuid = node_uuid
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'first_view', 'group_uuid': group_uuid, 'node_uuid': node_uuid}
            )
            self.events.append(event)
            return True

        # Process group change: old -> new
        group_change_match = re.search(r'Process\s+group\s+change:\s+([0-9a-f-]+)\s*->\s*([0-9a-f-]+)', line, re.IGNORECASE)
        if group_change_match and self.current_timestamp:
            old_uuid = group_change_match.group(1)
            new_uuid = group_change_match.group(2)
            self.cluster.group_uuid = new_uuid
            self.cluster.group_changes.append({
                'timestamp': self.current_timestamp,
                'from': old_uuid,
                'to': new_uuid
            })
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'group_change', 'from': old_uuid, 'to': new_uuid}
            )
            self.events.append(event)
            return True

        # State transfer required: parse Group state and Local state lines
        group_state_line = re.search(r'Group\s+state:\s+([0-9a-f-]+):(\-?\d+)', line, re.IGNORECASE)
        if group_state_line and self.current_timestamp:
            self.cluster.group_uuid = group_state_line.group(1)
            try:
                self.cluster.group_seqno = int(group_state_line.group(2))
            except Exception:
                self.cluster.group_seqno = None
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'st_group_state', 'group_uuid': self.cluster.group_uuid, 'group_seqno': self.cluster.group_seqno}
            )
            self.events.append(event)
            return True

        local_state_line = re.search(r'Local\s+state:\s+([0-9a-f-]+):(\-?\d+)', line, re.IGNORECASE)
        if local_state_line and self.current_timestamp:
            self.cluster.local_state_uuid = local_state_line.group(1)
            try:
                self.cluster.local_seqno = int(local_state_line.group(2))
            except Exception:
                self.cluster.local_seqno = None
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'st_local_state', 'local_state_uuid': self.cluster.local_state_uuid, 'local_seqno': self.cluster.local_seqno}
            )
            self.events.append(event)
            return True

        # Provider paused/resume lines
        provider_paused = re.search(r'Provider\s+paused\s+at\s+([0-9a-f-]+):(\-?\d+)', line, re.IGNORECASE)
        if provider_paused and self.current_timestamp:
            self.cluster.local_state_uuid = provider_paused.group(1)
            try:
                self.cluster.local_seqno = int(provider_paused.group(2))
            except Exception:
                self.cluster.local_seqno = None
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'provider_paused', 'uuid': self.cluster.local_state_uuid, 'seqno': self.cluster.local_seqno}
            )
            self.events.append(event)
            return True

        provider_resuming = re.search(r'resuming\s+provider\s+at\s+(\-?\d+)', line, re.IGNORECASE)
        if provider_resuming and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'provider_resuming', 'seqno': int(provider_resuming.group(1))}
            )
            self.events.append(event)
            return True

        provider_resumed = re.search(r'Provider\s+resumed\.', line, re.IGNORECASE)
        if provider_resumed and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'provider_resumed'}
            )
            self.events.append(event)
            return True

        # CC processing logs
        processing_cc = re.search(r'#######\s+processing\s+CC\s+(\d+).*(ordered|unordered)', line, re.IGNORECASE)
        if processing_cc and self.current_timestamp:
            cc_seq = int(processing_cc.group(1))
            order = processing_cc.group(2).lower()
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'processing_cc', 'seqno': cc_seq, 'order': order}
            )
            self.events.append(event)
            return True

        skipping_cc = re.search(r'#######\s+skipping\s+local\s+CC\s+(\d+),\s+keep\s+in\s+cache:\s+(true|false)', line, re.IGNORECASE)
        if skipping_cc and self.current_timestamp:
            cc_seq = int(skipping_cc.group(1))
            keep = skipping_cc.group(2).lower() == 'true'
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'skipping_cc', 'seqno': cc_seq, 'keep': keep}
            )
            self.events.append(event)
            return True

        # Identity mismatch fatal
        identity_mismatch = re.search(r'Node\s+UUID\s+([0-9a-f-]+)\s+is\s+absent\s+from\s+the\s+view', line, re.IGNORECASE)
        if identity_mismatch and self.current_timestamp:
            missing_uuid = identity_mismatch.group(1)
            event = LogEvent(
                event_type=EventType.ERROR,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'identity_mismatch', 'node_uuid': missing_uuid}
            )
            self.events.append(event)
            self.health_metrics.errors += 1
            return True

        # "####### My UUID: <uuid>"
        my_uuid_line = re.search(r'My\s+UUID:\s+([0-9a-f-]{36})', line, re.IGNORECASE)
        if my_uuid_line:
            self.cluster.node_instance_uuid = my_uuid_line.group(1)
            return True
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
        
        # gcs.cpp: Flow-control interval
        fc_interval = re.search(r'Flow-control\s+interval:\s*\[(\d+),\s*(\d+)\]', line, re.IGNORECASE)
        if fc_interval and self.current_timestamp:
            low = int(fc_interval.group(1))
            high = int(fc_interval.group(2))
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'flow_control_interval', 'lower': low, 'upper': high}
            )
            self.events.append(event)
            return True

        # gcs.cpp: FC STOP/CONT sent
        m = re.search(r'SENDING\s+FC_(STOP|CONT)\s+\(local\s+seqno:\s*(-?\d+),\s*fc_offset:\s*(-?\d+)\):\s*(-?\d+)', line, re.IGNORECASE)
        if m and self.current_timestamp:
            kind = m.group(1).upper()
            local_seqno = int(m.group(2))
            fc_offset = int(m.group(3))
            ret = int(m.group(4))
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': f'fc_{kind.lower()}', 'local_seqno': local_seqno, 'fc_offset': fc_offset, 'ret': ret}
            )
            self.events.append(event)
            return True

        # gcs.cpp: Replication paused due to SST queue hard limit
        if re.search(r'Replication\s+paused\s+until\s+state\s+transfer\s+is\s+complete', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'replication_paused', 'cause': 'sst_queue_hard_limit'}
            )
            self.events.append(event)
            return True

        # gcs.cpp: Sending/Not sending SYNC decisions
        if re.search(r'\bSENDING\s+SYNC\b', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'sync_sending'}
            )
            self.events.append(event)
            return True
        if re.search(r'\bNot\s+sending\s+SYNC\b', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'sync_not_sending'}
            )
            self.events.append(event)
            return True

        # gcs.cpp: Become joined/synced (with FC offset)
        bj = re.search(r'Become\s+joined,\s+FC\s+offset\s+(-?\d+)', line, re.IGNORECASE)
        if bj and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'become_joined', 'fc_offset': int(bj.group(1))}
            )
            self.events.append(event)
            return True
        bs = re.search(r'Become\s+synced,\s+FC\s+offset\s+(-?\d+)', line, re.IGNORECASE)
        if bs and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'become_synced', 'fc_offset': int(bs.group(1))}
            )
            self.events.append(event)
            return True

        # gcs.cpp: Received NON-PRIMARY / SELF-LEAVE
        if re.search(r'Received\s+NON-PRIMARY\.', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'received_non_primary'}
            )
            self.events.append(event)
            return True
        if re.search(r'Received\s+SELF-LEAVE\.', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'received_self_leave'}
            )
            self.events.append(event)
            return True

        # gcs.cpp: Channel open and failures
        open_chan = re.search(r"Opened\s+channel\s+'([^']+)'", line)
        if open_chan and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'channel_opened', 'channel': open_chan.group(1)}
            )
            self.events.append(event)
            return True
        open_fail = re.search(r"Failed\s+to\s+open\s+channel\s+'([^']+)'\s+at\s+'([^']+)':\s*(-?\d+)\s*\(([^)]+)\)", line)
        if open_fail and self.current_timestamp:
            chan = open_fail.group(1)
            url = open_fail.group(2)
            code = int(open_fail.group(3))
            err = open_fail.group(4)
            event = LogEvent(
                event_type=EventType.COMMUNICATION_ISSUE,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'channel_open_failed', 'channel': chan, 'url': url, 'code': code, 'error': err}
            )
            self.health_metrics.communication_issues += 1
            self.events.append(event)
            return True

        # gcs.cpp: RECV thread exiting
        if re.search(r'RECV\s+thread\s+exiting\s+-?\d+:', line, re.IGNORECASE) and self.current_timestamp:
            event = LogEvent(
                event_type=EventType.SERVER_INFO,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                metadata={'subtype': 'recv_thread_exit'}
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
            # If this server name matches our local node, record it
            if node_name and (self.cluster.local_node_name is None):
                # Prefer to set local node name only if we already know My UUID
                if self.cluster.node_instance_uuid:
                    self.cluster.local_node_name = node_name
            
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
        # Build IST summary from parsed IST events
        ist_events = [e for e in self.events if e.event_type == EventType.IST_EVENT]
        ist_summary: Dict[str, Any] = {}
        # Helper: build State Transfer workflows (IST attempt -> SST fallback -> IST catch-up)
        st_workflows: List[Dict[str, Any]] = []
        try:
            # Gather SST and IST subsets with timestamps for correlation
            sst_events = [e for e in self.events if e.event_type == EventType.SST_EVENT]
            sst_requests = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_requested']
            sst_starts = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_started']
            sst_failures = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_failed']
            sst_completed = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_completed']
            # IST indicators
            ist_sender_nothing = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_sender_nothing']
            ist_send_failed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_send_failed']
            ist_async_start = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_start']
            ist_completed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_completed']
            ist_async_served = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_served']
            ist_recv_prepared = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_prepared']

            # Simple time-based correlation: for each SST request (implies IST was not possible or insufficient),
            # collect surrounding IST failures before it, SST start/failure after, and IST async after SST.
            def ts_to_dt(ts: str) -> datetime:
                return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

            # Work through requests chronologically and assign IST events once
            sst_requests_sorted = sorted(sst_requests[-10:], key=lambda e: ts_to_dt(e.timestamp))
            used_async_ids: Set[int] = set()
            used_completed_ids: Set[int] = set()

            for req in sst_requests_sorted:  # inspect recent ones
                req_dt = ts_to_dt(req.timestamp)
                joiner = req.metadata.get('joiner')
                donor = req.metadata.get('donor')
                wf: Dict[str, Any] = {
                    'requested_at': req.timestamp,
                    'joiner': joiner,
                    'donor': donor,
                    'pre_ist_signals': [],
                    'sst': {},
                    'post_ist': {}
                }
                # Preceding IST failures or sender_nothing within 2 minutes
                for ev in (ist_sender_nothing + ist_send_failed):
                    try:
                        if 0 <= (req_dt - ts_to_dt(ev.timestamp)).total_seconds() <= 120:
                            wf['pre_ist_signals'].append({
                                'timestamp': ev.timestamp,
                                'type': ev.metadata.get('subtype')
                            })
                    except Exception:
                        pass
                # Find nearest SST start or failure within 5 minutes after request
                nearest_sst = None
                nearest_dt = None
                for ev in (sst_starts + sst_failures + sst_completed):
                    try:
                        dt = ts_to_dt(ev.timestamp)
                        if 0 <= (dt - req_dt).total_seconds() <= 300:
                            if nearest_dt is None or dt < nearest_dt:
                                nearest_dt = dt
                                nearest_sst = ev
                    except Exception:
                        pass
                if nearest_sst:
                    subtype = nearest_sst.metadata.get('subtype')
                    wf['sst'] = {
                        'timestamp': nearest_sst.timestamp,
                        'status': 'started' if subtype == 'sst_started' else ('failed' if subtype == 'sst_failed' else 'completed')
                    }
                # If we found a start, look for a completion shortly after
                if nearest_dt and sst_completed:
                    best_comp = None
                    best_comp_dt = None
                    for ev in sst_completed:
                        try:
                            cdt = ts_to_dt(ev.timestamp)
                            if cdt and 0 <= (cdt - nearest_dt).total_seconds() <= 900:
                                if best_comp_dt is None or cdt < best_comp_dt:
                                    best_comp = ev
                                    best_comp_dt = cdt
                        except Exception:
                            pass
                    if best_comp:
                        wf.setdefault('sst', {})['completed_at'] = best_comp.timestamp
                # Try to infer joiner listen address from nearest receiver_prepared before/after the SST request
                joiner_addr = None
                if req_dt:
                    best_gap = None
                    for ev in ist_recv_prepared:
                        try:
                            dt = ts_to_dt(ev.timestamp)
                            if dt and abs((dt - req_dt).total_seconds()) <= 600:
                                gap = abs((dt - req_dt).total_seconds())
                                if best_gap is None or gap < best_gap:
                                    best_gap = gap
                                    joiner_addr = ev.metadata.get('listen_addr')
                        except Exception:
                            pass
                if joiner_addr:
                    wf['joiner_addr'] = joiner_addr
                # Post-SST IST async start and completion within windows; assign each IST event only once
                post_start = None
                post_start_dt = None
                for ev in ist_async_start:
                    try:
                        ev_dt = ts_to_dt(ev.timestamp)
                        if ev_dt and nearest_dt and 0 <= (ev_dt - nearest_dt).total_seconds() <= 600 and id(ev) not in used_async_ids:
                            if post_start_dt is None or ev_dt < post_start_dt:
                                # If we know joiner_addr, prefer events whose peer matches that address
                                peer = ev.metadata.get('peer')
                                if joiner_addr and peer and joiner_addr.split(':')[1:] and peer.endswith(joiner_addr.split(':')[1]):
                                    post_start = ev
                                    post_start_dt = ev_dt
                                elif not joiner_addr:
                                    post_start = ev
                                post_start_dt = ev_dt
                    except Exception:
                        pass
                if post_start:
                    used_async_ids.add(id(post_start))
                    wf['post_ist']['async_start'] = {
                        'timestamp': post_start.timestamp,
                        'peer': post_start.metadata.get('peer'),
                        'first_seqno': post_start.metadata.get('first_seqno'),
                        'last_seqno': post_start.metadata.get('last_seqno')
                    }
                    if joiner_addr:
                        peer_val = post_start.metadata.get('peer') if post_start.metadata else None
                        try:
                            # Compare by ':port' suffix to be robust (tcp://host:port)
                            joiner_suffix = joiner_addr.split(':')[1] if ':' in joiner_addr else joiner_addr
                        except Exception:
                            joiner_suffix = None
                        wf['post_ist']['peer_matched'] = bool(peer_val and joiner_suffix and peer_val.endswith(joiner_suffix))
                post_comp = None
                post_comp_dt = None
                for ev in (ist_completed + ist_async_served):
                    try:
                        ev_dt = ts_to_dt(ev.timestamp)
                        if ev_dt and nearest_dt and 0 <= (ev_dt - nearest_dt).total_seconds() <= 900 and id(ev) not in used_completed_ids:
                            if post_comp_dt is None or ev_dt < post_comp_dt:
                                post_comp = ev
                                post_comp_dt = ev_dt
                    except Exception:
                        pass
                if post_comp:
                    used_completed_ids.add(id(post_comp))
                    wf['post_ist']['completed_at'] = post_comp.timestamp
                st_workflows.append(wf)
        except Exception:
            # Be resilient; if anything goes wrong, skip workflows
            st_workflows = []
        if ist_events:
            # Receiver side
            recv_prepared = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_prepared']
            applying_start = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_applying_start']
            completed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_completed']
            incomplete = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_incomplete']
            recv_addr = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_addr']
            recv_bind = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_bind']

            ist_summary['receiver'] = {}
            if recv_prepared:
                last = recv_prepared[-1]
                ist_summary['receiver']['prepared_range'] = {
                    'first_seqno': last.metadata.get('first_seqno'),
                    'last_seqno': last.metadata.get('last_seqno'),
                    'listen_addr': last.metadata.get('listen_addr'),
                    'timestamp': last.timestamp,
                }
            if applying_start:
                last = applying_start[-1]
                ist_summary['receiver']['applying_start'] = {
                    'first_seqno': last.metadata.get('first_seqno'),
                    'timestamp': last.timestamp,
                }
            if completed:
                ist_summary['receiver']['completed_at'] = completed[-1].timestamp
            if incomplete:
                last = incomplete[-1]
                ist_summary['receiver']['incomplete'] = {
                    'expected_last': last.metadata.get('expected_last'),
                    'last_received': last.metadata.get('last_received'),
                    'timestamp': last.timestamp,
                }
            if recv_addr:
                ist_summary['receiver']['recv_addr'] = recv_addr[-1].metadata.get('addr')
            if recv_bind:
                ist_summary['receiver']['recv_bind'] = recv_bind[-1].metadata.get('bind')

            # Sender side
            sender_ranges = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_sender_range']
            async_starts = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_start']
            failures = [e for e in ist_events if e.metadata and e.metadata.get('subtype') in ('ist_send_failed', 'async_ist_failed')]
            ist_summary['sender'] = {
                'ranges': [{
                    'first_seqno': e.metadata.get('first_seqno'),
                    'last_seqno': e.metadata.get('last_seqno'),
                    'timestamp': e.timestamp,
                    'node': (e.node.name if e.node else None)
                } for e in sender_ranges[-5:]],
                'async': [{
                    'peer': e.metadata.get('peer'),
                    'first_seqno': e.metadata.get('first_seqno'),
                    'last_seqno': e.metadata.get('last_seqno'),
                    'preload_start': e.metadata.get('preload_start'),
                    'timestamp': e.timestamp,
                } for e in async_starts[-5:]],
                'failures': [{
                    'reason': e.metadata.get('reason') or 'send failed',
                    'timestamp': e.timestamp,
                    'role': e.metadata.get('role')
                } for e in failures[-5:]],
            }
            ist_summary['counts'] = {
                'total': len(ist_events),
                'sender_ranges': len(sender_ranges),
                'async_starts': len(async_starts),
                'failures': len(failures),
            }

        return {
            "cluster_info": {
                "dialect": self.dialect,
                "total_nodes": self.cluster.node_count,
                "active_nodes": len(self.cluster.active_nodes),
                "current_view": asdict(self.cluster.current_view) if self.cluster.current_view else None,
                "total_views": len(self.cluster.views_history),
                "group_uuid": self.cluster.group_uuid,
                "group_seqno": self.cluster.group_seqno,
                "local_state_uuid": self.cluster.local_state_uuid,
                "local_seqno": self.cluster.local_seqno,
                "node_instance_uuid": self.cluster.node_instance_uuid,
                "local_node_name": self.cluster.local_node_name,
                "group_changes": self.cluster.group_changes[-5:]  # recent
            },
            "software": {
                "mariadb_version": self.software.get('mariadb_version'),
                "mariadb_edition": self.software.get('mariadb_edition'),
                "galera_version": self.software.get('wsrep_provider_version'),
                "galera_variant": self.software.get('galera_variant'),
                "wsrep_provider_path": self.software.get('wsrep_provider_path'),
                "galera_vendor": self.software.get('galera_vendor'),
            },
            "nodes": {node.name: asdict(node) for node in self.cluster.nodes.values()},
            "health_metrics": asdict(self.health_metrics),
            "health_summary": {
                "stability_score": self.health_metrics.stability_score,
                "health_status": self.health_metrics.health_status
            },
            "ist_summary": ist_summary if ist_events else None,
            "st_workflows": st_workflows if st_workflows else None,
            "timeline": {
                "total_events": len(self.events),
                "events_by_type": {event_type.value: len([e for e in self.events if e.event_type == event_type]) 
                                 for event_type in EventType}
            },
            "unknown": {
                "count": len(self._unknown_lines) if self.report_unknown else 0,
                "samples": self._unknown_lines[:5] if self.report_unknown else []
            } if self.report_unknown else None
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
    # Build summary first so we can show dialect in the header banner
    summary = analyzer.get_cluster_summary()
    cluster_info = summary["cluster_info"]
    software = summary.get("software", {})

    # Header banner
    lines.append("="*70)
    lines.append("GALERA CLUSTER LOG ANALYSIS")
    lines.append("="*70)
    # Header quick info (Dialect)
    # Compose dialect as: MariaDB <version>[ (<edition>)] [ + Galera <version>]
    mver = software.get('mariadb_version') if software else None
    med = software.get('mariadb_edition') if software else None
    gver = software.get('galera_version') if software else None
    if mver:
        dialect_display = f"MariaDB {mver}" + (f" ({med})" if med else "")
        if gver:
            dialect_display += f" + Galera {gver}"
    else:
        # Without MariaDB version, don't claim a dialect; show unknown
        dialect_display = 'unknown'
    lines.append(f"Dialect: {dialect_display}")
    
    # Cluster Overview
    lines.append("\n📊 CLUSTER OVERVIEW")
    lines.append("-" * 50)
    lines.append(f"Total Nodes Detected: {cluster_info['total_nodes']}")
    lines.append(f"Active Nodes: {cluster_info['active_nodes']}")
    lines.append(f"Total View Changes: {cluster_info['total_views']}")
    lines.append(f"Dialect: {dialect_display}")
    # Software versions
    if software:
        mver = software.get('mariadb_version')
        med = software.get('mariadb_edition')
        gver = software.get('galera_version')
        gvar = software.get('galera_variant')
        gvend = software.get('galera_vendor')
        if mver or med:
            if med:
                lines.append(f"MariaDB: {mver or '?'} ({med})")
            else:
                lines.append(f"MariaDB: {mver}")
        if gver or gvar or gvend:
            parts = [gver or '?']
            extras = []
            if gvar:
                extras.append(gvar)
            if gvend:
                extras.append(gvend)
            if software.get('galera_version_inferred') == 'true' and not gvend:
                extras.append('inferred')
            if extras:
                parts.append(f"({' / '.join(extras)})")
            lines.append("Galera: " + " ".join(parts))
    
    # Group/Local State section
    lines.append("\n🧭 GROUP STATE")
    lines.append("-" * 50)
    if cluster_info.get('group_uuid'):
        gseq = cluster_info.get('group_seqno')
        gseq_str = str(gseq) if gseq is not None else "?"
        lines.append(f"Group UUID: {cluster_info['group_uuid']} (seqno: {gseq_str})")
    if cluster_info.get('local_state_uuid') or cluster_info.get('local_seqno') is not None:
        lseq = cluster_info.get('local_seqno')
        lseq_str = str(lseq) if lseq is not None else "?"
        luuid = cluster_info.get('local_state_uuid') or 'unknown'
        lines.append(f"Local State: {luuid}:{lseq_str}")
    if cluster_info.get('node_instance_uuid'):
        lines.append(f"Node Instance UUID (My UUID): {cluster_info['node_instance_uuid']}")
    # Local node display for clarity
    local_name = cluster_info.get('local_node_name')
    if not local_name and cluster_info.get('node_instance_uuid'):
        # Try to resolve local name from nodes map
        try:
            node = analyzer.cluster.get_node_by_uuid(cluster_info.get('node_instance_uuid'))
            if node:
                local_name = node.name
        except Exception:
            pass
    if local_name:
        lines.append(f"Local node: {local_name}")
    if cluster_info.get('group_changes'):
        lines.append("Recent Group Changes:")
        for gc in cluster_info['group_changes']:
            lines.append(f"  {gc['timestamp']}: {gc['from']} -> {gc['to']}")
    
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
                timestamp = transition.timestamp  # Show full date and time
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
                    timestamp = transition.timestamp
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
    # Unknown lines
    if summary.get('unknown'):
        unk = summary['unknown']
        lines.append(f"Unknown WSREP/IST lines: {unk.get('count', 0)}")
        samples = unk.get('samples') or []
        for s in samples:
            lines.append(f"   ? {s[:120]}...")
    
    # Flow Control summary (derived from gcs.cpp patterns)
    fc_events = [e for e in analyzer.events if e.event_type == EventType.SERVER_INFO and e.metadata]
    fc_intervals = [e for e in fc_events if e.metadata.get('subtype') == 'flow_control_interval']
    fc_stop = [e for e in fc_events if e.metadata.get('subtype') == 'fc_stop']
    fc_cont = [e for e in fc_events if e.metadata.get('subtype') == 'fc_cont']
    fc_paused = [e for e in fc_events if e.metadata.get('subtype') == 'replication_paused']
    sync_send = [e for e in fc_events if e.metadata.get('subtype') == 'sync_sending']
    sync_skip = [e for e in fc_events if e.metadata.get('subtype') == 'sync_not_sending']
    become_joined = [e for e in fc_events if e.metadata.get('subtype') == 'become_joined']
    become_synced = [e for e in fc_events if e.metadata.get('subtype') == 'become_synced']
    
    if any([fc_intervals, fc_stop, fc_cont, fc_paused, sync_send, sync_skip, become_joined, become_synced]):
        lines.append("\n🛠️  FLOW CONTROL")
        lines.append("-" * 50)
        if fc_intervals:
            # Show the most recent interval
            last = fc_intervals[-1]
            low = last.metadata.get('lower')
            up = last.metadata.get('upper')
            lines.append(f"Interval: [{low}, {up}] (last seen {last.timestamp})")
        if fc_stop or fc_cont:
            lines.append(f"FC_STOP sent: {len(fc_stop)} | FC_CONT sent: {len(fc_cont)}")
            # Show last 2 signals for context
            for e in (fc_stop + fc_cont)[-2:]:
                kind = e.metadata.get('subtype', '').replace('fc_', '').upper()
                ls = e.metadata.get('local_seqno')
                off = e.metadata.get('fc_offset')
                lines.append(f"  {e.timestamp}: {kind} (local_seqno={ls}, fc_offset={off})")
        if fc_paused:
            lines.append(f"Replication paused events: {len(fc_paused)}")
        if sync_send or sync_skip:
            lines.append(f"SYNC decisions — sent: {len(sync_send)}, not sent: {len(sync_skip)}")
        if become_joined or become_synced:
            bj = become_joined[-1] if become_joined else None
            bs = become_synced[-1] if become_synced else None
            if bj:
                lines.append(f"Became JOINED (FC offset {bj.metadata.get('fc_offset')}): {bj.timestamp}")
            if bs:
                lines.append(f"Became SYNCED (FC offset {bs.metadata.get('fc_offset')}): {bs.timestamp}")
    
    # SST/IST Details with comprehensive donor/joiner information
    sst_events = [e for e in analyzer.events if e.event_type == EventType.SST_EVENT]
    ist_events = [e for e in analyzer.events if e.event_type == EventType.IST_EVENT]
    
    if sst_events or ist_events:
        # Pretty SST section like in README
        if sst_events:
            lines.append("\n💾 STATE SNAPSHOT TRANSFER (SST)")
            lines.append("-" * 50)
            sst_requests = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_requested']
            sst_starts = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_started']
            sst_failures = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_failed']

            # Helper to find nearest event after a given timestamp within a window
            def _ts_to_dt(ts: str):
                try:
                    return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    return None

            for req in sst_requests[-5:]:
                meta = req.metadata or {}
                joiner = meta.get('joiner', 'Unknown')
                donor = meta.get('donor', 'Unknown')
                # Correlate with nearest SST start to get method
                req_dt = _ts_to_dt(req.timestamp)
                method = None
                nearest_start = None
                nearest_start_dt = None
                # Try to find a recent stream address near the request
                nearest_addr = None
                nearest_addr_dt = None
                if req_dt:
                    for st in sst_starts:
                        st_dt = _ts_to_dt(st.timestamp)
                        if st_dt and 0 <= (st_dt - req_dt).total_seconds() <= 600:
                            if not nearest_start_dt or st_dt < nearest_start_dt:
                                nearest_start = st
                                nearest_start_dt = st_dt
                if nearest_start and nearest_start.metadata:
                    method = nearest_start.metadata.get('method')

                lines.append(f"  {req.timestamp} | SST REQUEST")
                if method:
                    lines.append(f"    └─ Method: {method}")
                lines.append(f"    └─ Donor: {donor}")
                lines.append(f"    └─ Joiner: {joiner}")
                # If we saw a recent streaming address, show it
                # Search in all SST stream_addr events
                stream_addrs = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_stream_addr']
                if req_dt and stream_addrs:
                    for ev in stream_addrs:
                        try:
                            dt = _ts_to_dt(ev.timestamp)
                            if dt and 0 <= (dt - req_dt).total_seconds() <= 600:
                                if not nearest_addr_dt or dt < nearest_addr_dt:
                                    nearest_addr = ev.metadata.get('joiner_addr')
                                    nearest_addr_dt = dt
                        except Exception:
                            pass
                if nearest_addr:
                    lines.append(f"    └─ Joiner Addr: {nearest_addr}")

                # Also show the first failure or start following the request
                # Prefer failures if they exist; otherwise show started
                nearest_fail = None
                nearest_fail_dt = None
                if req_dt:
                    for fl in sst_failures:
                        fl_dt = _ts_to_dt(fl.timestamp)
                        if fl_dt and 0 <= (fl_dt - req_dt).total_seconds() <= 900:
                            if not nearest_fail_dt or fl_dt < nearest_fail_dt:
                                nearest_fail = fl
                                nearest_fail_dt = fl_dt
                if nearest_fail:
                    lines.append(f"  {nearest_fail.timestamp} | SST FAILED")
                elif nearest_start:
                    lines.append(f"  {nearest_start.timestamp} | SST STARTED")

            # If no requests parsed, still show standalone starts/failures
            if not sst_requests:
                for st in sst_starts[-3:]:
                    method = (st.metadata or {}).get('method', 'unknown')
                    lines.append(f"  {st.timestamp} | SST STARTED ({method})")
                for fl in sst_failures[-3:]:
                    op = (fl.metadata or {}).get('operation', '')
                    code = (fl.metadata or {}).get('error_code', '')
                    extra = f" {op} {code}".strip()
                    lines.append(f"  {fl.timestamp} | SST FAILED{(' ('+extra+')') if extra else ''}")

        # Keep existing IST presentation next
        if ist_events:
            lines.append(f"\nIST Events ({len(ist_events)}):")
            # Compact summary (latest highlights)
            # Build a quick ist_summary like in JSON for display
            recv_prepared = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_prepared']
            async_starts = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_start']
            if recv_prepared:
                last = recv_prepared[-1]
                first = last.metadata.get('first_seqno')
                last_seq = last.metadata.get('last_seqno')
                addr = last.metadata.get('listen_addr')
                lines.append(f"  Latest Receiver prepared: {first}->{last_seq} at {addr} ({last.timestamp})")
            if async_starts:
                last = async_starts[-1]
                first = last.metadata.get('first_seqno')
                last_seq = last.metadata.get('last_seqno')
                preload = last.metadata.get('preload_start')
                peer = last.metadata.get('peer')
                lines.append(f"  Latest Async serve: {peer} {first}->{last_seq} (preload {preload}) ({last.timestamp})")
            # Summarize key IST activities
            recv_prepared = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_prepared']
            sender_ranges = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_sender_range']
            async_starts = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_start']
            incomplete = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_incomplete']
            send_failed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') in ('ist_send_failed', 'async_ist_failed')]
            completed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_completed']

            # Receiver prepared (range and listen address)
            for e in recv_prepared[-2:]:
                first = e.metadata.get('first_seqno')
                last = e.metadata.get('last_seqno')
                addr = e.metadata.get('listen_addr')
                node_name = e.node.name if e.node else 'Unknown'
                lines.append(f"  {e.timestamp}: [Receiver {node_name}] prepared for {first}->{last} at {addr}")

            # Sender ranges
            for e in sender_ranges[-3:]:
                first = e.metadata.get('first_seqno')
                last = e.metadata.get('last_seqno')
                node_name = e.node.name if e.node else 'Unknown'
                lines.append(f"  {e.timestamp}: [Sender {node_name}] sending {first}->{last}")

            # Async starts with preload
            for e in async_starts[-3:]:
                first = e.metadata.get('first_seqno')
                last = e.metadata.get('last_seqno')
                preload = e.metadata.get('preload_start')
                peer = e.metadata.get('peer')
                node_name = e.node.name if e.node else 'Unknown'
                lines.append(f"  {e.timestamp}: [Sender {node_name}] async serve {peer} {first}->{last} (preload {preload})")

            # Incomplete warnings
            for e in incomplete[-2:]:
                expected = e.metadata.get('expected_last')
                got = e.metadata.get('last_received')
                lines.append(f"  {e.timestamp}: [Receiver] IST incomplete (expected {expected}, got {got})")

            # Failures
            for e in send_failed[-2:]:
                reason = e.metadata.get('reason') or 'send failed'
                role = e.metadata.get('role', 'unknown')
                lines.append(f"  {e.timestamp}: [{role.title()}] IST failure: {reason}")

            # Completions (last one shown)
            if completed:
                lastc = completed[-1]
                lines.append(f"  {lastc.timestamp}: [Receiver] IST completed")

        # Summarize end-to-end State Transfer workflows
        st_workflows = summary.get('st_workflows')
        if st_workflows:
            lines.append("\n🧩 STATE TRANSFER WORKFLOWS")
            lines.append("-" * 50)
            for wf in st_workflows[-3:]:
                joiner = wf.get('joiner') or 'joiner'
                donor = wf.get('donor') or 'donor'
                lines.append(f"Request {wf.get('requested_at')}: {joiner} ⇐ {donor}")
                pre = wf.get('pre_ist_signals', [])
                if pre:
                    kinds = ", ".join(p.get('type', '') for p in pre)
                    lines.append(f"  Pre-IST: {kinds}")
                sst = wf.get('sst', {})
                if sst:
                    lines.append(f"  SST: {sst.get('status','?')} at {sst.get('timestamp','?')}")
                post = wf.get('post_ist', {})
                if post:
                    if 'async_start' in post:
                        a = post['async_start']
                        # Address and match indicator
                        addr = wf.get('joiner_addr')
                        match = post.get('peer_matched')
                        match_str = ' ✓' if match else (' ×' if match is False else '')
                        if addr:
                            lines.append(f"  Post-IST: async serve {a.get('peer')} {a.get('first_seqno')}→{a.get('last_seqno')} at {a.get('timestamp')} (joiner {addr}{match_str})")
                        else:
                            lines.append(f"  Post-IST: async serve {a.get('peer')} {a.get('first_seqno')}→{a.get('last_seqno')} at {a.get('timestamp')}{match_str}")
                    if 'completed_at' in post:
                        lines.append(f"  Post-IST: completed at {post['completed_at']}")
    
    # Recent Critical Events
    critical_events = [e for e in analyzer.events 
                      if e.event_type in [EventType.ERROR, EventType.COMMUNICATION_ISSUE]]
    if critical_events:
        lines.append("\n⚠️  CRITICAL EVENTS")
        lines.append("-" * 50)
        for event in critical_events[-8:]:  # Show last 8
            node_name = event.node.name if event.node else "Unknown"
            meta = event.metadata or {}
            subtype = meta.get('subtype')
            lines.append(f"{event.timestamp}: [{node_name}] {event.event_type.value.upper()}")
            if subtype == 'aborted_connection':
                db = meta.get('db') or ''
                user = meta.get('user') or ''
                host = meta.get('host') or ''
                cid = meta.get('connection_id')
                lines.append(f"  └─ Aborted connection #{cid}")
                lines.append(f"     └─ DB: {db if db else '-'}")
                lines.append(f"     └─ User: {user if user else '-'}")
                lines.append(f"     └─ Host: {host if host else '-'}")
            else:
                # Fallback: show truncated raw line
                lines.append(f"   {event.raw_message[:120]}...")
    
    return "\n".join(lines)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Galera Log Analyzer',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('logfile', nargs='?', help='Galera log file to analyze')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--filter', help='Filter events by type (comma-separated)')
    parser.add_argument('--dialect', default='auto', help='Log dialect: auto|galera-26|mariadb-10|pxc-8|galera|unknown')
    parser.add_argument('--report-unknown', action='store_true', help='Report unknown WSREP/IST lines')
    parser.add_argument('--mariadb-version', help='MariaDB server version (e.g., 10.6.16, 11.4.7-4)')
    parser.add_argument('--mariadb-edition', choices=['enterprise', 'community'], help='MariaDB edition')
    parser.add_argument('--galera-version', help='Galera wsrep provider version (e.g., 26.4.22)')
    
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
    analyzer = GaleraLogAnalyzer(
        dialect=args.dialect,
        report_unknown=bool(getattr(args, 'report_unknown', False)),
        mariadb_version=getattr(args, 'mariadb_version', None),
        mariadb_edition=getattr(args, 'mariadb_edition', None),
        galera_version=getattr(args, 'galera_version', None),
    )
    analyzer.parse_log(log_lines)

    # Infer Galera from MariaDB if missing
    analyzer.infer_galera_from_mariadb()

    # Require MariaDB version known; Galera optional/inferred
    sw = analyzer.software
    have_mariadb = bool(sw.get('mariadb_version'))
    if not have_mariadb:
        print("Error: MariaDB version unknown. Provide via --mariadb-version (and optionally --mariadb-edition), or include version lines in the log.", file=sys.stderr)
        sys.exit(2)
    
    # Output results
    if args.format == 'json':
        summary = analyzer.get_cluster_summary()
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(output_text(analyzer))

if __name__ == "__main__":
    main()
