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
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum

class DialectRegistry:
    """
    Registry for dialect-specific parsing patterns.
    Starts with a 'default' dialect containing current patterns,
    allows gradual addition of version-specific patterns without breaking existing code.
    """
    
    def __init__(self):
        self.dialects: Dict[str, Dict[str, Dict[str, Any]]] = {
            'default': self._build_default_patterns()
        }
        # Pattern resolution cache for performance
        self._pattern_cache: Dict[str, Any] = {}
    
    def _build_default_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Build the default pattern set from current implementation"""
        return {
            # ===== IST PATTERNS (Incremental State Transfer) =====
            'ist_patterns': {
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
                'recv_summary_seqnos': re.compile(r'Receiving\s+IST:\s+\d+\s+writesets,\s+seqnos\s+(-?\d+)\s*-\s*(-?\d+)', re.IGNORECASE),
                'ist_uuid_range': re.compile(r'IST\s+uuid:\s*[0-9a-f-]+,?\s*f:\s*(-?\d+),\s*l:\s*(-?\d+)', re.IGNORECASE),
                'processing_complete': re.compile(r'Processing\s+event\s+queue:.*?100\.0%.*?complete\.?', re.IGNORECASE),
                # Additional IST patterns from scattered code
                'ist_receiver_addr_simple': re.compile(r'IST receiver addr(?: using)? ([0-9.]+)', re.IGNORECASE),
                'async_serve_tcp': re.compile(r'async IST sender starting to serve tcp://([0-9.]+)', re.IGNORECASE),
                'ist_detection': re.compile(r'(?:\bIST\b|incremental\s+state\s+transfer)', re.IGNORECASE),
            },
            
            # ===== SST PATTERNS (State Snapshot Transfer) =====
            'sst_patterns': {
                # SST Request and selection patterns
                'request_with_donor': re.compile(r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+requested\s+state\s+transfer\s+from\s+[\'\"]([^\'\"]+)[\'\"]\.\s+Selected\s+(\d+\.\d+)\s+\(([^)]+)\)\(([^)]+)\)\s+as\s+donor', re.IGNORECASE),
                'simple_request': re.compile(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer', re.IGNORECASE),
                'request_from': re.compile(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer from', re.IGNORECASE),
                
                # SST Role and initialization patterns
                'joiner_role': re.compile(r"wsrep_sst_[a-z0-9_]+.*--role\s+'joiner'.*--address\s+'([0-9.]+)", re.IGNORECASE),
                'donor_role': re.compile(r"wsrep_sst_[a-z0-9_]+.*--role\s+'donor'.*--address\s+'([0-9.]+)", re.IGNORECASE),
                'initiating_transfer': re.compile(r"Initiating\s+SST/IST\s+transfer\s+on\s+(DONOR|JOINER)\s+side\s*\((wsrep_sst_[a-z0-9_]+)\s+.*?--role\s+'(donor|joiner)'.*?--address\s+'([^']+)'", re.IGNORECASE),
                
                # SST Status patterns
                'started': re.compile(r'WSREP_SST:\s+\[INFO\]\s+(\w+)\s+SST\s+started\s+on\s+(donor|joiner)\s+\(([^)]+)\)', re.IGNORECASE),
                'completed': re.compile(r"WSREP_SST:\s+\[INFO\]\s+(\w+)\s+SST\s+completed\s+on\s+(donor|joiner)\s+\(([^)]+)\)", re.IGNORECASE),
                'proceeding': re.compile(r'WSREP_SST:\s+\[INFO\]\s+Proceeding\s+with\s+SST', re.IGNORECASE),
                
                # SST Failure patterns (Enhanced)
                'reject_in_state': re.compile(r"Rejecting\s+State\s+Transfer\s+Request\s+in\s+state\s+'([^']+)'.", re.IGNORECASE),
                'reject_wrong_state': re.compile(r'Received\s+State\s+Transfer\s+Request\s+in\s+wrong\s+state\s+([A-Z/\-]+)\.\s+Rejecting\.', re.IGNORECASE),
                'provider_failed': re.compile(r'State\s+transfer\s+to\s+(\d+\.\d+)\s+\(([^)]+)\)\s+failed:\s+(.+)$', re.IGNORECASE),
                'sending_failed': re.compile(r'SST\s+(sending|receiving)\s+failed:\s*(-?\d+)', re.IGNORECASE),
                'abort_never_receive': re.compile(r'Will\s+never\s+receive\s+state\.\s+Need\s+to\s+abort\.', re.IGNORECASE),
                'sst_request_failed': re.compile(r'SST\s+request\s+failed:\s*(.+)$', re.IGNORECASE),
                'donor_error': re.compile(r'DONOR\s+ERROR:\s*(.+)$', re.IGNORECASE),
                'joiner_error': re.compile(r'JOINER\s+ERROR:\s*(.+)$', re.IGNORECASE),
                'sst_timeout': re.compile(r'SST\s+timeout', re.IGNORECASE),
                'sst_connection_failed': re.compile(r'SST\s+connection\s+failed', re.IGNORECASE),
                
                # SST Connection and streaming
                'stream_addr': re.compile(r"SST\s+request\s+sent,\s+waiting\s+for\s+connection\s+at\s+([^'\s]+)", re.IGNORECASE),
                'sent_uuid_seqno': re.compile(r"SST\s+sent:\s+([0-9a-f-]+):(\-?\d+)", re.IGNORECASE),
                
                # SST Timing patterns
                'total_time_joiner': re.compile(r"Joiner\s+monitor\s+thread\s+ended\s+with\s+total\s+time\s+(\d+)\s+sec", re.IGNORECASE),
                'total_time_donor': re.compile(r"Donor\s+monitor\s+thread\s+ended\s+with\s+total\s+time\s+(\d+)\s+sec", re.IGNORECASE),
            },
            
            # ===== STATE TRANSITION PATTERNS =====
            'state_transition_patterns': {
                'shifting': re.compile(r'Shifting\s+([A-Z/]+)\s*->\s*([A-Z/]+)\s*\(TO:\s*(\d+)\)', re.IGNORECASE),
                'restored': re.compile(r'Restored\s+state\s+([A-Z/\-]+)\s*->\s*([A-Z/\-]+)\s*\(([-\d]+)\)', re.IGNORECASE),
                'node_state': re.compile(r'Node\s+([0-9a-f-]+)\s+state\s+(\w+)', re.IGNORECASE),
                'member_status': re.compile(r'Member\s+(\d+\.\d+)\s+\(([^)]+)\)\s+(\w+)', re.IGNORECASE),
                'become_joined': re.compile(r'Become\s+joined,\s+FC\s+offset\s+(-?\d+)', re.IGNORECASE),
                'become_synced': re.compile(r'Become\s+synced,\s+FC\s+offset\s+(-?\d+)', re.IGNORECASE),
            },
            
            # ===== VIEW CHANGE PATTERNS =====
            'view_change_patterns': {
                # Main view pattern
                'view_main': re.compile(r'view\(view_id\((\w+),\s*([^,]+),\s*(\d+)\)', re.IGNORECASE),
                'member_line': re.compile(r'^\s*([0-9a-f-]+),(\d+)\s*$', re.MULTILINE),
                
                # Detailed view parsing
                'view_id_detailed': re.compile(r'^\s*id:\s*([0-9a-f-]{36}):(\d+)', re.MULTILINE),
                'view_status': re.compile(r'^\s*status:\s*(\w+)', re.MULTILINE),
                'protocol_version': re.compile(r'^\s*protocol_version:\s*(\d+)', re.MULTILINE),
                'capabilities': re.compile(r'^\s*capabilities:\s*(.+)', re.MULTILINE),
                
                # View processing
                'first_view': re.compile(r'Process\s+first\s+view:\s+([0-9a-f-]+)\s+my\s+uuid:\s+([0-9a-f-]+)', re.IGNORECASE),
                'group_change': re.compile(r'Process\s+group\s+change:\s+([0-9a-f-]+)\s*->\s*([0-9a-f-]+)', re.IGNORECASE),
            },
            
            # ===== COMMUNICATION PATTERNS =====
            'communication_patterns': {
                'connection_established': re.compile(r'connection established to ([0-9a-f]{8}(?:-[0-9a-f]{4})?) tcp://([0-9a-fA-F:.]+)', re.IGNORECASE),
                'server_connected': re.compile(r"Server ([A-Za-z0-9_.-]+) connected to cluster at position [0-9a-f:-]+ with ID ([0-9a-f]{8}-[0-9a-f]{4,})", re.IGNORECASE),
                'listening_uuid': re.compile(r"\(([0-9a-f]{8}(?:-[0-9a-f]{4}){0,3}-?[0-9a-f]{0,12}), *'tcp://([0-9a-fA-F:.]+)'\) listening at tcp://\2", re.IGNORECASE),
                'cluster_address': re.compile(r'wsrep_cluster_address=gcomm://([0-9.:,]+)', re.IGNORECASE),
                'state_exchange_sent': re.compile(r'STATE_EXCHANGE:\s+(sent|got)\s+state\s+(UUID|msg):\s+([0-9a-f-]+)', re.IGNORECASE),
                'state_exchange_from': re.compile(r'from\s+(\d+)\s+\(([^)]+)\)', re.IGNORECASE),
            },
            
            # ===== SERVER INFO PATTERNS =====
            'server_info_patterns': {
                # Version and banner patterns
                'server_version': re.compile(r'Server\s+version:\s*([^\s]+)', re.IGNORECASE),
                'mariadb_banner': re.compile(r'Starting\s+MariaDB\s+([^\s]+).*?source\s+revision\s+([0-9a-f]{40})', re.IGNORECASE),
                'enterprise_path': re.compile(r'mariadb-enterprise-([0-9][0-9\.-]+)', re.IGNORECASE),
                
                # WSREP Provider info
                'provider_version': re.compile(r'WSREP:\s+Provider:\s+Galera\s+([0-9][0-9\.]+)', re.IGNORECASE),
                'provider_version_alt': re.compile(r'wsrep[_\s-]*provider[_\s-]*version[^0-9]*([0-9]+(?:\.[0-9]+)+)', re.IGNORECASE),
                'wsrep_load': re.compile(r'wsrep_load\(\):\s*Galera\s+([0-9][\w\.-]+)(?:\([^)]*\))?\s+by\s+([^\r\n]+)', re.IGNORECASE),
                
                # State and UUID info
                'my_uuid': re.compile(r'My\s+UUID:\s+([0-9a-f-]{36})', re.IGNORECASE),
                'group_state': re.compile(r'Group\s+state:\s+([0-9a-f-]+):(\-?\d+)', re.IGNORECASE),
                'local_state': re.compile(r'Local\s+state:\s+([0-9a-f-]+):(\-?\d+)', re.IGNORECASE),
                'uuid_name_pair': re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}),\s*([A-Za-z0-9_-]+)', re.IGNORECASE),
                'indexed_uuid_name': re.compile(r'\b\d+:\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}),\s*([A-Za-z0-9_-]+)', re.IGNORECASE),
                'node_name_sst': re.compile(r"wsrep_node_name=([A-Za-z0-9_.-]+)", re.IGNORECASE),
            },
            
            # ===== FLOW CONTROL PATTERNS =====
            'flow_control_patterns': {
                'fc_stop_cont': re.compile(r'SENDING\s+FC_(STOP|CONT)\s+\(local\s+seqno:\s*(-?\d+),\s*fc_offset:\s*(-?\d+)\):\s*(-?\d+)', re.IGNORECASE),
                'provider_paused': re.compile(r'Provider\s+paused\s+at\s+([0-9a-f-]+):(\-?\d+)', re.IGNORECASE),
                'provider_resuming': re.compile(r'resuming\s+provider\s+at\s+(\-?\d+)', re.IGNORECASE),
                'processing_cc': re.compile(r'#######\s+processing\s+CC\s+(\d+).*(ordered|unordered)', re.IGNORECASE),
                'skipping_cc': re.compile(r'#######\s+skipping\s+local\s+CC\s+(\d+),\s+keep\s+in\s+cache:\s+(true|false)', re.IGNORECASE),
            },
            
            # ===== GENERAL PATTERNS =====
            'general_patterns': {
                'timestamp': re.compile(r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})', re.IGNORECASE),
                'dialect_detection_pxc': re.compile(r'PXC|Percona XtraDB Cluster', re.IGNORECASE),
                'dialect_detection_galera': re.compile(r'Galera\s+26\.', re.IGNORECASE),
                'dialect_detection_mariadb': re.compile(r'Maria(DB)?\s+10\.', re.IGNORECASE),
                'dialect_detection_wsrep': re.compile(r'WSREP', re.IGNORECASE),
            },
            
            # ===== SERVICE EVENT PATTERNS =====
            'service_event_patterns': {
                # Server startup patterns (actual MariaDB/MySQL server lifecycle)
                'server_startup': re.compile(r'Starting\s+(MariaDB|MySQL)(?:\s+([0-9][0-9\.-]+))?', re.IGNORECASE),
                'mysqld_starting': re.compile(r'(mariadbd|mysqld).*starting', re.IGNORECASE),
                'server_ready': re.compile(r'^(?!.*WSREP:).*ready\s+for\s+connections', re.IGNORECASE),  # Exclude WSREP messages
                'server_version_start': re.compile(r'Starting\s+(MariaDB|MySQL)\s+\d+\.\d+', re.IGNORECASE),
                
                # Shutdown patterns (actual server shutdown)
                'shutdown_signal': re.compile(r'received\s+SHUTDOWN_SIGNAL', re.IGNORECASE),
                'shutdown_complete': re.compile(r'(mariadbd|mysqld):\s*shutdown\s+complete', re.IGNORECASE),
                'normal_shutdown': re.compile(r'Normal\s+shutdown', re.IGNORECASE),
                'shutting_down': re.compile(r'Shutting\s+down', re.IGNORECASE),
                
                # Crash/Signal patterns - updated for actual log formats
                'signal_received': re.compile(r'signal\s+(\d+)\s+received', re.IGNORECASE),
                'fatal_signal': re.compile(r'Fatal\s+signal\s+(\d+)', re.IGNORECASE),
                'mariadb_got_signal': re.compile(r'(mariadbd|mysqld)\s+got\s+signal\s+(\d+)', re.IGNORECASE),
                'segfault': re.compile(r'segmentation\s+fault|SIGSEGV', re.IGNORECASE),
                'abort_signal': re.compile(r'SIGABRT|abort', re.IGNORECASE),
                'killed_signal': re.compile(r'SIGKILL|killed', re.IGNORECASE),
                
                # Restart detection patterns
                'restart_detected': re.compile(r'mysqld\s+restarted', re.IGNORECASE),
                'process_id_change': re.compile(r'Process\s+ID\s+(\d+)', re.IGNORECASE),
            },
        }
    
    def get_patterns(self, dialect: str, pattern_category: str) -> Dict[str, Any]:
        """
        Get patterns for a specific dialect and category.
        Falls back to 'default' if dialect-specific patterns don't exist.
        """
        cache_key = f"{dialect}:{pattern_category}"
        if cache_key in self._pattern_cache:
            return self._pattern_cache[cache_key]
        
        # Try dialect-specific patterns first
        if dialect in self.dialects and pattern_category in self.dialects[dialect]:
            patterns = self.dialects[dialect][pattern_category]
        # Fall back to default
        elif pattern_category in self.dialects['default']:
            patterns = self.dialects['default'][pattern_category]
        else:
            patterns = {}
        
        self._pattern_cache[cache_key] = patterns
        return patterns
    
    def add_dialect_patterns(self, dialect: str, pattern_category: str, patterns: Dict[str, Any]):
        """Add or update patterns for a specific dialect"""
        if dialect not in self.dialects:
            self.dialects[dialect] = {}
        self.dialects[dialect][pattern_category] = patterns
        # Clear cache for this category
        keys_to_remove = [k for k in self._pattern_cache.keys() if k.endswith(f":{pattern_category}")]
        for key in keys_to_remove:
            del self._pattern_cache[key]
    
    def list_available_dialects(self) -> List[str]:
        """List all available dialects"""
        return list(self.dialects.keys())
    
    def list_pattern_categories(self, dialect: str = 'default') -> List[str]:
        """List available pattern categories for a dialect"""
        if dialect in self.dialects:
            return list(self.dialects[dialect].keys())
        return list(self.dialects['default'].keys())
    
    def add_dialect_variant(self, dialect: str, base_dialect: str = 'default'):
        """Create a new dialect based on another dialect's patterns"""
        if base_dialect in self.dialects:
            import copy
            self.dialects[dialect] = copy.deepcopy(self.dialects[base_dialect])
        else:
            self.dialects[dialect] = self._build_default_patterns()
    
    def update_pattern(self, dialect: str, category: str, pattern_name: str, pattern):
        """Update a specific pattern for a dialect"""
        if dialect not in self.dialects:
            self.add_dialect_variant(dialect)
        if category not in self.dialects[dialect]:
            self.dialects[dialect][category] = {}
        self.dialects[dialect][category][pattern_name] = pattern
        # Clear cache
        cache_key = f"{dialect}:{category}"
        self._pattern_cache.pop(cache_key, None)

class EventType(Enum):
    SERVER_INFO = "server_info"
    CLUSTER_VIEW = "cluster_view"
    STATE_TRANSITION = "state_transition"
    SST_EVENT = "sst_event"
    IST_EVENT = "ist_event"
    COMMUNICATION_ISSUE = "communication_issue"
    SERVICE_EVENT = "service_event"  # startup, shutdown, crash
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
    placeholder: bool = False      # True if this name is a temporary placeholder
    uuid_history: List[str] = field(default_factory=list)  # Historical UUIDs this logical node used (full UUIDs)
    
    def __hash__(self):
        return hash(self.uuid)
    
    def __eq__(self, other):
        if isinstance(other, Node):
            return self.uuid == other.uuid
        return False

class NodeIdentityRegistry:
    """Collects evidence for (uuid -> name) mappings and resolves the best name.
    Evidence sources are weighted; higher score wins. Once a high score (>=80) is set, only higher replaces it."""
    def __init__(self):
        self._evidence: Dict[str, Dict[str, Any]] = {}
        # Weight definitions
        self._weights = {
            'sst_request': 90,
            'member_synced': 85,
            'server_sync': 85,
            'membership_view': 70,
            'donor_selection': 90,
            'fallback': 10,
        }

    def add(self, uuid: str, name: str, source: str, timestamp: Optional[str] = None):
        if not uuid or not name:
            return
        score = self._weights.get(source, 50)
        rec = self._evidence.get(uuid)
        if rec is None:
            self._evidence[uuid] = {
                'name': name,
                'score': score,
                'source': source,
                'first_seen': timestamp,
                'last_seen': timestamp,
                'sources': {source}
            }
            return
        # Update existing
        rec['last_seen'] = timestamp or rec.get('last_seen')
        rec['sources'].add(source)
        # Replace name if higher score
        if score > rec['score']:
            rec['name'] = name
            rec['score'] = score
            rec['source'] = source

    def resolve(self) -> Dict[str, str]:
        return {u: info['name'] for u, info in self._evidence.items()}

    def get_info(self, uuid: str) -> Optional[Dict[str, Any]]:
        return self._evidence.get(uuid)

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
    # Alias mapping: short patterns -> full UUID (e.g., first8-first4th)
    uuid_aliases: Dict[str, str] = field(default_factory=dict)
    # Candidate local UUIDs gleaned from 'listening at' lines before a deterministic mapping to physical name
    candidate_local_uuids: List[str] = field(default_factory=list)
    
    def add_node(self, node: Node) -> None:
        """Add a node to the cluster"""
        self.nodes[node.uuid] = node
        # Register alias patterns for UUID shortening (first segment + fourth segment)
        parts = node.uuid.split('-')
        if len(parts) == 5:
            first8 = parts[0]
            fourth = parts[3]
            composite = f"{first8}-{fourth}"
            # Map both first8 and composite to full UUID if unique
            if first8 not in self.uuid_aliases:
                self.uuid_aliases[first8] = node.uuid
            if composite not in self.uuid_aliases:
                self.uuid_aliases[composite] = node.uuid
    
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
class DowntimePeriod:
    """Represents a period where the cluster was down or degraded"""
    start_time: str
    end_time: str
    duration_seconds: float
    duration_str: str
    gap_type: str  # 'no_primary', 'no_activity', 'node_departure', 'cluster_restart'
    description: str
    severity: str  # 'low', 'medium', 'high', 'critical'

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
    downtime_periods: List[DowntimePeriod] = field(default_factory=list)
    total_downtime_seconds: float = 0.0
    
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
        
        # Initialize dialect registry
        self.dialect_registry = DialectRegistry()
        
        # Future usage examples (commented out for reference):
        # # For MariaDB 10.6+ with different IST messages
        # self.dialect_registry.add_dialect_variant('mariadb-10.6')
        # self.dialect_registry.update_pattern('mariadb-10.6', 'ist', 'start_pattern', 
        #     r'.*Incremental state transfer required.*')
        # 
        # # For Percona XtraDB Cluster 8.0
        # self.dialect_registry.add_dialect_variant('pxc-8.0')
        # self.dialect_registry.update_pattern('pxc-8.0', 'ist', 'complete_pattern',
        #     r'.*IST process completed successfully.*')
        
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
        # Identity registry
        self.identity_registry = NodeIdentityRegistry()
        # IP evidence: uuid/name key -> {ip: (score, first_ts, last_ts, count)}
        self._ip_evidence: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._ip_weights = {
            'listening': 100,         # (uuid,'tcp://ip') listening at tcp://ip
            'connection_established': 90,  # connection established to peer
            'sst_joiner': 95,         # joiner own --address
            'sst_donor_peer': 60,     # donor sees joiner address
            'ist_receiver_addr': 80,  # IST receiver addr using ip
            'ist_async_peer': 50,     # async IST sender peer
            'cluster_address': 40,    # wsrep_cluster_address list
        }

    @property
    def _ist_patterns(self) -> Dict[str, Any]:
        """Get IST patterns for current dialect (backward-compatible property)"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'ist_patterns')
    
    @property
    def _sst_patterns(self) -> Dict[str, Any]:
        """Get SST patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'sst_patterns')
    
    @property
    def _state_transition_patterns(self) -> Dict[str, Any]:
        """Get state transition patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'state_transition_patterns')
    
    @property
    def _view_change_patterns(self) -> Dict[str, Any]:
        """Get view change patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'view_change_patterns')
    
    @property
    def _communication_patterns(self) -> Dict[str, Any]:
        """Get communication patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'communication_patterns')
    
    @property
    def _server_info_patterns(self) -> Dict[str, Any]:
        """Get server info patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'server_info_patterns')
    
    @property
    def _flow_control_patterns(self) -> Dict[str, Any]:
        """Get flow control patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'flow_control_patterns')
    
    @property
    def _general_patterns(self) -> Dict[str, Any]:
        """Get general patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'general_patterns')
    
    @property
    def _service_event_patterns(self) -> Dict[str, Any]:
        """Get service event patterns for the current dialect"""
        return self.dialect_registry.get_patterns(self.resolved_dialect, 'service_event_patterns')
    
    @property
    def resolved_dialect(self) -> str:
        """Get the resolved dialect, falling back to 'default' if unknown"""
        if self.dialect in ['auto', 'unknown']:
            return 'default'
        return self.dialect

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
        # Finalize node identities after full pass
        self._finalize_identities()

    def _finalize_identities(self) -> None:
        mapping = self.identity_registry.resolve()
        # Apply mapping to permanent nodes
        for uuid, best_name in mapping.items():
            perm = self.cluster.get_node_by_uuid(uuid)
            if perm and best_name and (perm.placeholder or perm.name.startswith('Temp-')):
                perm.name = best_name
                perm.placeholder = False
            elif not perm:
                self.cluster.add_node(Node(uuid=uuid, name=best_name, placeholder=False))

        # Helper to expand short UUID if unique
        def expand_short(u: str) -> str:
            if len(u) == 12 and u.count('-') == 1:  # pattern like abcd1234-efgh
                # Fall through; treat as already truncated meaningful id
                return u
            if len(u) < 36:
                candidates = [full for full in self.cluster.nodes.keys() if full.startswith(u)]
                if len(candidates) == 1:
                    return candidates[0]
            return u

        # Patch historical views
        for view in self.cluster.views_history:
            updated_members: Dict[str, Node] = {}
            for muuid, mnode in list(view.members.items()):
                # First, try alias map (composite or first segment)
                aliased = self.cluster.uuid_aliases.get(muuid)
                full_uuid = aliased if aliased else expand_short(muuid)
                perm = self.cluster.get_node_by_uuid(full_uuid)
                if perm:
                    # Replace mapping with permanent node object
                    updated_members[full_uuid] = perm
                    # Update name via identity mapping if needed
                    if full_uuid in mapping:
                        perm.name = mapping[full_uuid]
                        perm.placeholder = False
                    elif (perm.name.startswith('Temp-') or perm.placeholder) and not mnode.name.startswith('Temp-'):
                        perm.name = mnode.name
                        perm.placeholder = False
                else:
                    # Keep existing temp node (upgrade name if mapping exists)
                    if muuid in mapping:
                        mnode.name = mapping[muuid]
                        mnode.placeholder = False
                    updated_members[muuid] = mnode
            view.members = updated_members
        # Second pass: fill remaining placeholders (N/A or Temp- names) with resolved names if available
        for view in self.cluster.views_history:
            for muuid, mnode in view.members.items():
                perm = self.cluster.get_node_by_uuid(muuid)
                if perm and (mnode.name == 'N/A' or mnode.name.startswith('Temp-')) and not perm.name.startswith('Temp-'):
                    mnode.name = perm.name
                    mnode.placeholder = False
        # After identities stable, resolve IP evidence to assign addresses
        self._finalize_ips(mapping)
        # Collapse identical consecutive views now that names stabilized
        self._collapse_identical_views()
        # Replace synthetic name-derived UUIDs (node_<name>_<n>) with real UUIDs seen in views
        synthetic_pattern = re.compile(r'^node_[A-Za-z0-9_.-]+_\d+$')
        # Build name->real_uuid candidates from views
        name_real_uuid: Dict[str, str] = {}
        for view in self.cluster.views_history:
            for m in view.members.values():
                if m and m.name and m.uuid and m.uuid.count('-') == 4 and len(m.uuid) >= 36:
                    # Keep first discovered mapping per name
                    name_real_uuid.setdefault(m.name, m.uuid)
        for node in list(self.cluster.nodes.values()):
            if synthetic_pattern.match(node.uuid):
                real_uuid = name_real_uuid.get(node.name)
                if real_uuid and real_uuid != node.uuid:
                    # Update uuid history
                    if real_uuid not in node.uuid_history:
                        node.uuid_history.append(real_uuid)
                    # Re-index cluster.nodes dict
                    try:
                        del self.cluster.nodes[node.uuid]
                    except Exception:
                        pass
                    node.uuid = real_uuid
                    self.cluster.nodes[real_uuid] = node

    def _collapse_identical_views(self):
        if not self.cluster.views_history:
            return
        collapsed = []
        prev_sig = None
        for view in self.cluster.views_history:
            # Signature: status + sorted member UUIDs (full) + optional protocol_version
            member_ids = sorted([m.uuid for m in view.members.values()])
            sig = (view.status, tuple(member_ids), view.protocol_version)
            if sig == prev_sig:
                continue  # skip duplicate
            collapsed.append(view)
            prev_sig = sig
        # Replace history if reduced
        if len(collapsed) != len(self.cluster.views_history):
            self.cluster.views_history = collapsed

    def _record_ip(self, key: str, ip: str, source: str, ts: Optional[str]):
        if not key or not ip:
            return
        # Normalize ip (strip protocol and trailing punctuation)
        ip_norm = ip
        ip_norm = re.sub(r'^tcp://', '', ip_norm)
        ip_norm = ip_norm.split(',')[0]
        score = self._ip_weights.get(source, 10)
        recs = self._ip_evidence.setdefault(key, {})
        rec = recs.get(ip_norm)
        if rec is None:
            recs[ip_norm] = {
                'score': score,
                'first_seen': ts,
                'last_seen': ts,
                'count': 1,
                'sources': {source}
            }
        else:
            rec['last_seen'] = ts or rec['last_seen']
            rec['count'] += 1
            rec['sources'].add(source)
            # Upgrade score if higher
            if score > rec['score']:
                rec['score'] = score

    def _finalize_ips(self, mapping: Dict[str, str]):
        # Keys can be UUIDs or names (for joiner lines before UUID known)
        for key, ipmap in self._ip_evidence.items():
            # Choose best ip: highest score, then most counts, then earliest first_seen
            best = None
            for ip, meta in ipmap.items():
                if best is None:
                    best = (ip, meta)
                else:
                    b_ip, b_meta = best
                    if meta['score'] > b_meta['score'] or (
                        meta['score'] == b_meta['score'] and (
                            meta['count'] > b_meta['count'] or (
                                meta['count'] == b_meta['count'] and (meta['first_seen'] or '') < (b_meta['first_seen'] or '')
                            ))):
                        best = (ip, meta)
            if not best:
                continue
            chosen_ip = best[0]
            # Resolve key to node
            node = self.cluster.get_node_by_uuid(key) or self.cluster.get_node_by_name(key)
            if not node:
                # Try expand short key via aliases
                full = self.cluster.uuid_aliases.get(key)
                if full:
                    node = self.cluster.get_node_by_uuid(full)
            if node and (not node.address or node.address == 'ip:unknown') and chosen_ip != '0.0.0.0':
                node.address = chosen_ip

    def _retroactively_fill_name(self, full_uuid: str, real_name: str) -> None:
        """When we learn a node's real name after views were logged with placeholders,
        rewrite historical view member entries (Temp-/N/A) for that UUID or its short/composite aliases.
        Low complexity: iterate views history, locate matching keys (full UUID, first8, composite first8-fourth),
        and patch Node objects in-place.
        """
        if not full_uuid or not real_name:
            return
        parts = full_uuid.split('-')
        first8 = parts[0] if parts else full_uuid[:8]
        composite = None
        if len(parts) == 5:
            composite = f"{parts[0]}-{parts[3]}"
        # Ensure permanent node updated
        perm = self.cluster.get_node_by_uuid(full_uuid)
        if perm and (perm.placeholder or perm.name.startswith('Temp-') or perm.name == 'N/A' or perm.name.startswith('Node-')):
            perm.name = real_name
            perm.placeholder = False
        # Walk historical views
        for view in self.cluster.views_history:
            # If full UUID present
            node_obj = view.members.get(full_uuid)
            if node_obj and (node_obj.name.startswith('Temp-') or node_obj.name == 'N/A'):
                node_obj.name = real_name
                node_obj.placeholder = False
            # Short / composite keys may have been used originally; replace mapping
            # Collect keys to adjust to avoid modifying during iteration
            adjust_keys = []
            for k, v in view.members.items():
                if k == full_uuid:
                    continue
                if (k == first8 or (composite and k == composite)) and (v.name.startswith('Temp-') or v.name == 'N/A' or v.placeholder):
                    adjust_keys.append(k)
            for k in adjust_keys:
                old = view.members.pop(k)
                # Insert or overwrite with full UUID pointing to permanent node if exists
                view.members[full_uuid] = perm if perm else old
                view.members[full_uuid].name = real_name
                view.members[full_uuid].placeholder = False
        # Register aliases for future lookups if not already
        if first8 and first8 not in self.cluster.uuid_aliases:
            self.cluster.uuid_aliases[first8] = full_uuid
        if composite and composite not in self.cluster.uuid_aliases:
            self.cluster.uuid_aliases[composite] = full_uuid

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
        # Early local node detection from state transfer request lines
        # NOTE: This detection can be ambiguous in multi-node scenarios where
        # logs contain references to multiple nodes. See TROUBLESHOOTING_NODE_DETECTION.md
        # for details on when automatic detection fails and manual mapping is needed.
        if not self.cluster.local_node_name and 'requested state transfer' in line:
            m_local = re.search(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer', line)
            if m_local:
                self.cluster.local_node_name = m_local.group(1)
        
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
        if self._parse_service_event(line):
            return
        if self._parse_communication_issue(line):
            return
        if self._parse_error(line):
            return
        if self._parse_warning(line):
            return
        if self._parse_server_info(line):
            return
        if not self.cluster.local_node_name:
            m_local = re.search(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer from', line)
            if m_local:
                self.cluster.local_node_name = m_local.group(1)
        # Always attempt passive IP evidence collection (non-blocking)
        try:
            self._parse_ip_evidence(line)
        except Exception:
            pass

    def _parse_ip_evidence(self, line: str) -> None:
        """Collect multi-source IP evidence with weighted scoring.
        This runs opportunistically on every line after primary parsing so it does not interfere with other handlers.
        Sources (descending authority):
          listening:    (UUID,'tcp://IP') listening at tcp://IP
          connection_established: connection established to <shortUUID> tcp://IP:port
          sst_joiner:   wsrep_sst_mariabackup --role 'joiner' --address 'IP[:port]'
          sst_donor_peer: donor-side reference to joiner address
          ist_receiver_addr: IST receiver addr=IP (from earlier planned pattern; adapt when encountered)
          ist_async_peer: async IST sender starting to serve tcp://IP
          cluster_address: wsrep_cluster_address=gcomm://IP1,IP2,... (weak evidence)
        Keys can be UUIDs, short/composite UUIDs, or temporary names until identity resolution finalizes.
        """
        if not line or 'tcp' not in line and 'IST' not in line and 'wsrep_sst' not in line and 'gcomm://' not in line:
            return  # fast path
        ts = self.current_timestamp
        # (UUID,'tcp://IP') listening at tcp://IP
        m = re.search(r"\(([0-9a-f]{8}(?:-[0-9a-f]{4}){0,3}-?[0-9a-f]{0,12}), *'tcp://([0-9a-fA-F:.]+)'\) listening at tcp://\2", line)
        if m:
            uuid_like = m.group(1)
            ip = m.group(2)
            self._record_ip(uuid_like, ip, 'listening', ts)
            # Always treat 'listening at' UUID as local candidate (highest certainty about local endpoint)
            if uuid_like and uuid_like not in self.cluster.candidate_local_uuids:
                self.cluster.candidate_local_uuids.append(uuid_like)
            return
        # Server <NAME> connected to cluster ... with ID <UUID>
        m = re.search(r"Server ([A-Za-z0-9_.-]+) connected to cluster at position [0-9a-f:-]+ with ID ([0-9a-f]{8}-[0-9a-f]{4,})", line)
        if m:
            name = m.group(1)
            raw_uuid = m.group(2)
            if not self.cluster.local_node_name:
                self.cluster.local_node_name = name
            if raw_uuid not in self.cluster.candidate_local_uuids:
                self.cluster.candidate_local_uuids.append(raw_uuid)
        # connection established to <short/composite> tcp://IP:PORT
        m = re.search(r'connection established to ([0-9a-f]{8}(?:-[0-9a-f]{4})?) tcp://([0-9a-fA-F:.]+)', line)
        if m:
            self._record_ip(m.group(1), m.group(2), 'connection_established', ts)
            return
        # wsrep_sst_mariabackup --role 'joiner' --address 'IP[:port]'
        m = re.search(r"wsrep_sst_[a-z0-9_]+.*--role +'joiner'.*--address +'([0-9.]+)" , line, re.IGNORECASE)
        if m:
            ip = m.group(1)
            # Try to capture node name on same line if present (wsrep_node_name=NAME)
            name_match = re.search(r"wsrep_node_name=([A-Za-z0-9_.-]+)", line)
            key = name_match.group(1) if name_match else 'joiner'
            self._record_ip(key, ip, 'sst_joiner', ts)
            return
        # donor side referencing joiner address
        m = re.search(r"wsrep_sst_[a-z0-9_]+.*--role +'donor'.*--address +'([0-9.]+)" , line, re.IGNORECASE)
        if m:
            self._record_ip('joiner', m.group(1), 'sst_donor_peer', ts)
            return
        # IST receiver addr=IP (variant patterns: 'IST receiver addr using IP' already parsed elsewhere but add fallback)
        m = re.search(r'IST receiver addr(?: using)? ([0-9.]+)', line, re.IGNORECASE)
        if m:
            self._record_ip('ist_receiver', m.group(1), 'ist_receiver_addr', ts)
            return
        # async IST sender starting to serve tcp://IP
        m = re.search(r'async IST sender starting to serve tcp://([0-9.]+)', line, re.IGNORECASE)
        if m:
            self._record_ip('ist_sender', m.group(1), 'ist_async_peer', ts)
            return
        # wsrep_cluster_address=gcomm://IP1,IP2
        m = re.search(r'wsrep_cluster_address=gcomm://([0-9.:,]+)', line)
        if m:
            for ip in m.group(1).split(','):
                ip_s = ip.strip()
                if ip_s:
                    self._record_ip(ip_s, ip_s, 'cluster_address', ts)
    
    def _extract_node_info(self, line: str) -> None:
        """Extract and maintain node information from log line"""
        # Secondary local node detection (redundant safeguard)
        if not self.cluster.local_node_name and 'requested state transfer' in line:
            m_local2 = re.search(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer', line)
            if m_local2:
                self.cluster.local_node_name = m_local2.group(1)
        # Extract UUID and node name pairs from member lists (most accurate)
        uuid_name_match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}),\s*([A-Za-z0-9_-]+)', line)
        if uuid_name_match:
            uuid = uuid_name_match.group(1)
            name = uuid_name_match.group(2)
            if name and not name.upper() in ['INFO', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'TRACE', 'NOTE']:
                # Ensure the node exists with the given UUID/name
                self._ensure_node_exists_with_uuid(name, uuid)
                # Retroactively patch historical views with real name
                self._retroactively_fill_name(uuid, name)
                # If this UUID matches our local instance UUID, capture local node name
                if self.cluster.node_instance_uuid and uuid == self.cluster.node_instance_uuid:
                    self.cluster.local_node_name = name
                # Do NOT set current_node_name from membership lines to avoid misattribution
                return

        # Pattern: index: fulluuid, NAME  (e.g., '1: 4d718057-923b-11f0-85f2-e281f60451d3, UAT-DB-01')
        indexed_uuid_name = re.search(r'\b\d+:\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}),\s*([A-Za-z0-9_-]+)', line)
        if indexed_uuid_name:
            uuid = indexed_uuid_name.group(1)
            name = indexed_uuid_name.group(2)
            if name and not name.upper() in ['INFO', 'WARN', 'WARNING', 'ERROR', 'DEBUG', 'TRACE', 'NOTE']:
                self._ensure_node_exists_with_uuid(name, uuid)
                # Add higher confidence evidence
                self.identity_registry.add(uuid, name, 'member_synced', self.current_timestamp)
                # Retroactively patch historical views with real name
                self._retroactively_fill_name(uuid, name)
                # If current view exists and has a placeholder entry matching the short form, update it
                if self.cluster.current_view:
                    short_key = uuid.split('-')[0]
                    for m_uuid, m_node in list(self.cluster.current_view.members.items()):
                        if m_uuid.startswith(short_key) and (m_node.name.startswith('Temp-') or m_node.name == 'N/A'):
                            m_node.name = name
                            m_node.placeholder = False
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
            new_node = Node(uuid=node_uuid, name=node_name, uuid_history=[node_uuid])
            self.cluster.add_node(new_node)
            return new_node
        return existing_node
    
    def _ensure_node_exists_with_uuid(self, node_name: str, uuid: str) -> Node:
        """Ensure a node exists with specific UUID, collecting identity evidence."""
        existing_node = self.cluster.get_node_by_uuid(uuid)
        if existing_node:
            if existing_node.name != node_name:
                existing_node.name = node_name
                self.identity_registry.add(uuid, node_name, 'membership_view', self.current_timestamp)
            if uuid not in existing_node.uuid_history:
                existing_node.uuid_history.append(uuid)
            return existing_node

        existing_by_name = self.cluster.get_node_by_name(node_name)
        if existing_by_name:
            old_uuid = existing_by_name.uuid
            if old_uuid != uuid and old_uuid in self.cluster.nodes:
                del self.cluster.nodes[old_uuid]
            # Track history
            if old_uuid and old_uuid not in existing_by_name.uuid_history:
                existing_by_name.uuid_history.append(old_uuid)
            existing_by_name.uuid = uuid
            if uuid not in existing_by_name.uuid_history:
                existing_by_name.uuid_history.append(uuid)
            self.cluster.nodes[uuid] = existing_by_name
            self.identity_registry.add(uuid, existing_by_name.name, 'membership_view', self.current_timestamp)
            return existing_by_name

        new_node = Node(uuid=uuid, name=node_name, uuid_history=[uuid])
        self.cluster.add_node(new_node)
        self.identity_registry.add(uuid, node_name, 'membership_view', self.current_timestamp)
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
    
    def _get_sst_error_context(self, error_code: str) -> str:
        """Provide context for SST error codes"""
        error_map = {
            '-2': 'ENOENT - File or directory not found (data directory issue)',
            '-4': 'EINTR - Interrupted system call (process interrupted)',
            '-5': 'EIO - I/O error (disk or network issue)',
            '-12': 'ENOMEM - Out of memory',
            '-13': 'EACCES - Permission denied (file/directory permissions)',
            '-22': 'EINVAL - Invalid argument (configuration issue)',
            '-32': 'EPIPE - Broken pipe (network connection lost)',
            '-104': 'ECONNRESET - Connection reset by peer',
            '-110': 'ETIMEDOUT - Connection timed out',
            '-111': 'ECONNREFUSED - Connection refused (donor not available)',
            '-113': 'EHOSTUNREACH - No route to host',
            '1': 'Generic error',
            '2': 'Script execution failed',
            '3': 'Script configuration error',
            '22': 'Script parameter error',
            '125': 'Script not found',
            '126': 'Script not executable',
            '127': 'Script command not found'
        }
        return error_map.get(error_code, f'Unknown error code: {error_code}')
    
    def _parse_cluster_view(self, line: str) -> bool:
        """Parse cluster view/membership changes"""
        # Look for view definitions with detailed membership
        view_match = re.search(r'view\(view_id\((\w+),\s*([^,]+),\s*(\d+)\)', line)
        if view_match and self.current_timestamp:
            status = view_match.group(1).lower()  # PRIM or NON_PRIM
            view_uuid = view_match.group(2)
            view_seq = view_match.group(3)
            view_id = f"{view_uuid}.{view_seq}"
            # Deduplicate consecutive identical provisional view ids
            last_view = self.cluster.views_history[-1] if self.cluster.views_history else None
            if not last_view or last_view.view_id != view_id:
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

        # Enhanced detailed View: block parsing (multi-line provider block)
        # Detect lines like "View:" then subsequent indented metadata lines:
        #   id: <uuid>:<seq>
        #   status: primary
        #   protocol_version: 4
        #   capabilities: CAP1, CAP2, ...
        #   members(N):
        # This block appears later and contains the authoritative view id which differs from the earlier short form.
        # We parse id/status/protocol/capabilities lines independently and patch the latest current_view.
        detailed_id = re.search(r'^\s*id:\s*([0-9a-f-]{36}):(\d+)', line)
        if detailed_id and self.cluster.current_view:
            full_uuid = detailed_id.group(1)
            seq = detailed_id.group(2)
            # Override earlier synthetic view_id
            self.cluster.current_view.view_id = f"{full_uuid}:{seq}"
            return True
        detailed_status = re.search(r'^\s*status:\s*(\w+)', line)
        if detailed_status and self.cluster.current_view:
            self.cluster.current_view.status = detailed_status.group(1).lower()
            return True
        proto_ver = re.search(r'^\s*protocol_version:\s*(\d+)', line)
        if proto_ver and self.cluster.current_view:
            self.cluster.current_view.protocol_version = proto_ver.group(1)
            return True
        caps = re.search(r'^\s*capabilities:\s*(.+)', line)
        if caps and self.cluster.current_view:
            # Split by comma and strip
            caps_list = [c.strip() for c in caps.group(1).split(',') if c.strip()]
            self.cluster.current_view.capabilities = caps_list
            return True
        # members(N): handled indirectly via indexed member lines elsewhere
        
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
        
        # Look for WSREP state transitions (both node states and cluster component states)
        # "Shifting SYNCED -> DONOR/DESYNCED (TO: 1625)"
        # "Shifting PRIMARY -> JOINER (TO: 1586)"
        # "Shifting CLOSED -> OPEN (TO: 0)"
        shifting_match = re.search(r'Shifting\s+([A-Z/]+)\s*->\s*([A-Z/]+)\s*\(TO:\s*(\d+)\)', line, re.IGNORECASE)
        if shifting_match:
            # All shifting transitions are legitimate state changes for visualization
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
    # Example: "Member 1.1 (node-01) requested state transfer from '*any*'. Selected 0.1 (node-03)(SYNCED) as donor."
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

        # WSREP_SST proceeding with SST (strong joiner-side indicator of full SST)
        if re.search(r'WSREP_SST:\s+\[INFO\]\s+Proceeding\s+with\s+SST', line, re.IGNORECASE) and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.events.append(LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_proceeding', 'role': 'joiner'}
            ))
            return True

        # WSREP_SST joiner housekeeping signals
        if re.search(r'WSREP_SST:\s+\[INFO\]\s+Cleaning\s+the\s+existing\s+datadir', line, re.IGNORECASE) and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.events.append(LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_clean_datadir'}
            ))
            return True
        if re.search(r'WSREP_SST:\s+\[INFO\]\s+Cleaning\s+the\s+old\s+binary\s+logs', line, re.IGNORECASE) and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.events.append(LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_clean_binlogs'}
            ))
            return True
        if re.search(r'WSREP_SST:\s+\[INFO\]\s+Waiting\s+for\s+SST\s+streaming\s+to\s+complete', line, re.IGNORECASE) and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.events.append(LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_wait_stream'}
            ))
            return True

        # WSREP_SST: Galera co-ords from donor: <uuid>:<seqno> <flags>
        m = re.search(r'WSREP_SST:\s+\[INFO\]\s+Galera\s+co-ords\s+from\s+donor:\s+([0-9a-f-]+):(\-?\d+)\s+\d+', line, re.IGNORECASE)
        if m and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.events.append(LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_galera_coords', 'uuid': m.group(1), 'seqno': int(m.group(2))}
            ))
            return True

        # WSREP_SST helper lines that explicitly indicate IST-only path using mariabackup/xtrabackup_ist
        ist_helper = re.search(r"WSREP_SST:\s+\[INFO\]\s+'?xtrabackup_ist'?\s+received\s+from\s+donor:.*Running\s+IST", line, re.IGNORECASE)
        if ist_helper and self.current_timestamp:
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_helper_ist_path', 'status': 'ist_only'}
            )
            self.events.append(event)
            return True

        # Bookkeeping lines sometimes printed even during IST-only flows
        # e.g., "SST received", "SST succeeded", "Installed new state from SST" with old seqno
        if self.current_timestamp and re.search(r'\bSST\s+(received|succeeded)\b', line, re.IGNORECASE):
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_helper_bookkeeping'}
            )
            self.events.append(event)
            return True
        if self.current_timestamp and re.search(r'Installed\s+new\s+state\s+from\s+SST', line, re.IGNORECASE):
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_helper_installed_state'}
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
        
        # Pattern 3: SST Failures (Enhanced with error context)
        # "SST sending failed: -32"
        failed_match = re.search(r'SST\s+(sending|receiving)\s+failed:\s*(-?\d+)', line, re.IGNORECASE)
        if failed_match:
            if self.current_timestamp:
                operation = failed_match.group(1)  # sending or receiving
                error_code = failed_match.group(2)
                
                self.health_metrics.sst_events += 1
                self.health_metrics.errors += 1
                
                # Enhanced error context based on common error codes
                error_context = self._get_sst_error_context(error_code)
                
                sst_info = {
                    'subtype': 'sst_failed',
                    'operation': operation,
                    'error_code': error_code,
                    'error_description': error_context,
                    'status': 'failed',
                    'severity': 'high' if operation == 'receiving' else 'medium'
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

        # Provider-side: State transfer to X (name) failed: <reason>
        prov_fail = re.search(r'State\s+transfer\s+to\s+\d+\.\d+\s+\([^)]+\)\s+failed:\s+(.+)$', line, re.IGNORECASE)
        if prov_fail and self.current_timestamp:
            reason = prov_fail.group(1).strip()
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_failed', 'operation': 'provider', 'reason': reason, 'status': 'failed'}
            )
            self.events.append(event)
            return True

        # Fatal abort: Will never receive state. Need to abort. => treat as SST failure for joiner attempt
        abort_fail = re.search(r'Will\s+never\s+receive\s+state\.\s+Need\s+to\s+abort\.', line, re.IGNORECASE)
        if abort_fail and self.current_timestamp:
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_failed', 'operation': 'abort', 'reason': 'Will never receive state', 'status': 'failed'}
            )
            self.events.append(event)
            return True

        # Additional SST failure patterns (Enhanced detection)
        # SST request failed
        sst_req_fail = self._sst_patterns['sst_request_failed'].search(line)
        if sst_req_fail and self.current_timestamp:
            reason = sst_req_fail.group(1).strip()
            self.health_metrics.sst_events += 1
            self.health_metrics.errors += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={
                    'subtype': 'sst_failed', 
                    'operation': 'request', 
                    'reason': reason, 
                    'status': 'failed',
                    'severity': 'high'
                }
            )
            self.events.append(event)
            return True

        # Donor/Joiner specific errors
        donor_error = self._sst_patterns['donor_error'].search(line)
        if donor_error and self.current_timestamp:
            error_msg = donor_error.group(1).strip()
            self.health_metrics.sst_events += 1
            self.health_metrics.errors += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={
                    'subtype': 'sst_failed', 
                    'operation': 'donor', 
                    'error_message': error_msg, 
                    'status': 'failed',
                    'severity': 'medium'
                }
            )
            self.events.append(event)
            return True

        joiner_error = self._sst_patterns['joiner_error'].search(line)
        if joiner_error and self.current_timestamp:
            error_msg = joiner_error.group(1).strip()
            self.health_metrics.sst_events += 1
            self.health_metrics.errors += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={
                    'subtype': 'sst_failed', 
                    'operation': 'joiner', 
                    'error_message': error_msg, 
                    'status': 'failed',
                    'severity': 'high'
                }
            )
            self.events.append(event)
            return True

        # SST timeout
        if self._sst_patterns['sst_timeout'].search(line) and self.current_timestamp:
            self.health_metrics.sst_events += 1
            self.health_metrics.errors += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={
                    'subtype': 'sst_failed', 
                    'operation': 'timeout', 
                    'reason': 'SST operation timed out', 
                    'status': 'failed',
                    'severity': 'high'
                }
            )
            self.events.append(event)
            return True
        
        # Pattern 4: General Member messages with node IDs and status
    # Example: "Member 0.1 (node-03) synced with group."
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
        
        # Pattern 5a: Total time on donor/joiner
        total_time = re.search(r"WSREP_SST:\s+\[INFO\]\s+Total\s+time\s+on\s+(donor|joiner):\s+(\d+)\s+seconds", line, re.IGNORECASE)
        if total_time and self.current_timestamp:
            secs = int(total_time.group(2))
            side = total_time.group(1).lower()
            self.health_metrics.sst_events += 1
            event = LogEvent(
                event_type=EventType.SST_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'sst_total_time', 'seconds': secs, 'side': side}
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

        # Applier finished processing queue (100.0% complete) => consider IST completion
        if self._ist_patterns['processing_complete'].search(line):
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_completed_applier', 'role': 'receiver'}
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

        # Authoritative receiver-side range summary
        m = self._ist_patterns['recv_summary_seqnos'].search(line)
        if m:
            first, last = int(m.group(1)), int(m.group(2))
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_recv_summary', 'first_seqno': first, 'last_seqno': last, 'role': 'receiver'}
            ))
            return True

        # IST uuid: f: <first>, l: <last>
        m = self._ist_patterns['ist_uuid_range'].search(line)
        if m:
            first, last = int(m.group(1)), int(m.group(2))
            self.health_metrics.ist_events += 1
            self.events.append(LogEvent(
                event_type=EventType.IST_EVENT,
                timestamp=ts,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'ist_uuid_range', 'first_seqno': first, 'last_seqno': last, 'role': 'receiver'}
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
    
    def _parse_service_event(self, line: str) -> bool:
        """Parse service lifecycle events (startup, shutdown, crash)"""
        if not self.current_timestamp:
            return False
        
        patterns = self._service_event_patterns
        
        # Check for startup events
        if patterns['server_startup'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'startup', 'event': 'server_start'}
            )
            self.events.append(event)
            return True
        
        # Check for additional server startup patterns
        if patterns['mysqld_starting'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'startup', 'event': 'mysqld_starting'}
            )
            self.events.append(event)
            return True
        
        if patterns['server_ready'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'startup', 'event': 'server_ready'}
            )
            self.events.append(event)
            return True
        
        # Check for shutdown events
        if patterns['shutdown_signal'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'shutdown', 'event': 'shutdown_signal'}
            )
            self.events.append(event)
            return True
        
        if patterns['normal_shutdown'].search(line) or patterns['shutdown_complete'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'shutdown', 'event': 'normal_shutdown'}
            )
            self.events.append(event)
            return True
        
        # Check for crash/signal events
        signal_match = patterns['signal_received'].search(line)
        if signal_match:
            signal_num = signal_match.group(1)
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'crash', 'event': 'signal_received', 'signal': signal_num}
            )
            self.events.append(event)
            return True
        
        fatal_signal_match = patterns['fatal_signal'].search(line)
        if fatal_signal_match:
            signal_num = fatal_signal_match.group(1)
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'crash', 'event': 'fatal_signal', 'signal': signal_num}
            )
            self.events.append(event)
            return True
        
        # Check for mariadb/mysqld got signal pattern
        mariadb_signal_match = patterns['mariadb_got_signal'].search(line)
        if mariadb_signal_match:
            signal_num = mariadb_signal_match.group(2)
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'crash', 'event': 'mariadb_got_signal', 'signal': signal_num}
            )
            self.events.append(event)
            return True
        
        if patterns['segfault'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'crash', 'event': 'segmentation_fault'}
            )
            self.events.append(event)
            return True
        
        if patterns['restart_detected'].search(line):
            event = LogEvent(
                event_type=EventType.SERVICE_EVENT,
                timestamp=self.current_timestamp,
                raw_message=line.strip(),
                node=self._get_local_node(),
                metadata={'subtype': 'restart', 'event': 'restart_detected'}
            )
            self.events.append(event)
            return True
        
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

        # Starting MariaDB <full-version> ... source revision <hash>
        # Example: "Starting MariaDB 10.6.22-18-MariaDB-enterprise-log source revision 188731e0649d75d061c8887e3a0d34d4fa8f4982"
        start_banner = re.search(r'Starting\s+MariaDB\s+([^\s]+).*?source\s+revision\s+([0-9a-f]{40})', line, re.IGNORECASE)
        if start_banner:
            full = start_banner.group(1)
            rev = start_banner.group(2)
            # Record full banner regardless of existing CLI overrides, but don't overwrite basic version if already set
            self.software['mariadb_full_banner'] = f"{full} source revision {rev}"
            if not self.software.get('mariadb_version'):
                # Extract numeric prefix for version field only
                m = re.match(r'([0-9]+(?:\.[0-9]+){1,3})', full)
                if m:
                    self.software['mariadb_version'] = m.group(1)
            # Edition inference from banner
            if 'enterprise' in full.lower():
                self.software['mariadb_edition'] = self.software.get('mariadb_edition') or 'enterprise'

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
            # Record cluster name (channel) if not already set
            if not self.cluster.name:
                self.cluster.name = open_chan.group(1)
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
    # Example: "Server node-03 synced with group"
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
        # Purge accidental nodes like 'offset' created from parsing non-node lines
        if 'offset' in self.cluster.nodes:
            try:
                del self.cluster.nodes['offset']
            except Exception:
                pass
        # Build IST summary from parsed IST events
        ist_events = [e for e in self.events if e.event_type == EventType.IST_EVENT]
        ist_summary: Dict[str, Any] = {}
        # Helper: build State Transfer workflows (IST attempt -> SST fallback -> IST catch-up)
        st_workflows: List[Dict[str, Any]] = []
        try:
            # Gather SST and IST subsets with timestamps for correlation
            sst_events = [e for e in self.events if e.event_type == EventType.SST_EVENT]
            sst_requests = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_requested']
            sst_starts = [e for e in sst_events if e.metadata and e.metadata.get('subtype') in ('sst_started','sst_proceeding')]
            sst_failures = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_failed']
            sst_completed = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_completed']
            sst_helper_ist = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_helper_ist_path']
            sst_bookkeeping = [e for e in sst_events if e.metadata and e.metadata.get('subtype') in ('sst_helper_bookkeeping','sst_helper_installed_state')]
            sst_proceedings = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_proceeding']
            sst_clean_datadir = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_clean_datadir']
            sst_wait_stream = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_wait_stream']
            sst_stream_addrs = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_stream_addr']
            sst_sents = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_sent']
            sst_total_time_donor = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_total_time' and e.metadata.get('side') == 'donor']
            sst_complete_notices = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_complete_notice']
            sst_coords = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_galera_coords']
            # IST indicators
            ist_sender_nothing = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_sender_nothing']
            ist_send_failed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_send_failed']
            ist_async_start = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_start']
            ist_completed = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_completed']
            ist_async_served = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'async_ist_served']
            ist_recv_prepared = [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_receiver_prepared']
            ist_recv_summary = [e for e in ist_events if e.metadata and e.metadata.get('subtype') in ('ist_recv_summary','ist_uuid_range','ist_applying_start')]

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
                    'post_ist': {},
                    'decision': None
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
                # Find SST start/failure/complete events in windows after request
                sst_start_ev = None
                sst_start_dt = None
                for ev in sst_starts:
                    try:
                        dt = ts_to_dt(ev.timestamp)
                        if 0 <= (dt - req_dt).total_seconds() <= 600:
                            if sst_start_dt is None or dt < sst_start_dt:
                                sst_start_dt = dt
                                sst_start_ev = ev
                    except Exception:
                        pass
                sst_fail_ev = None
                sst_fail_dt = None
                for ev in sst_failures:
                    try:
                        dt = ts_to_dt(ev.timestamp)
                        if 0 <= (dt - req_dt).total_seconds() <= 1800:
                            if sst_fail_dt is None or dt < sst_fail_dt:
                                sst_fail_dt = dt
                                sst_fail_ev = ev
                    except Exception:
                        pass
                sst_comp_ev = None
                sst_comp_dt = None
                if sst_start_dt:
                    for ev in sst_completed:
                        try:
                            cdt = ts_to_dt(ev.timestamp)
                            if cdt and 0 <= (cdt - sst_start_dt).total_seconds() <= 900:
                                if sst_comp_dt is None or cdt < sst_comp_dt:
                                    sst_comp_dt = cdt
                                    sst_comp_ev = ev
                        except Exception:
                            pass
                # Populate SST block if any
                if sst_start_ev:
                    wf['sst'] = {
                        'timestamp': sst_start_ev.timestamp,
                        'status': 'started'
                    }
                if sst_fail_ev:
                    wf.setdefault('sst', {})
                    wf['sst'].setdefault('timestamp', sst_fail_ev.timestamp)
                    wf['sst']['status'] = 'failed'
                if sst_comp_ev:
                    wf.setdefault('sst', {})
                    wf['sst']['completed_at'] = sst_comp_ev.timestamp
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
                # Attach nearby Galera coordinates from donor to workflow for clarity
                if req_dt and sst_coords:
                    best = None
                    best_dt = None
                    for ev in sst_coords:
                        try:
                            dt = ts_to_dt(ev.timestamp)
                            if dt and 0 <= abs((dt - req_dt).total_seconds()) <= 600:
                                if best_dt is None or abs((dt - req_dt).total_seconds()) < abs((best_dt - req_dt).total_seconds()):
                                    best_dt = dt
                                    best = ev
                        except Exception:
                            pass
                    if best:
                        wf['galera_coords'] = {'uuid': best.metadata.get('uuid'), 'seqno': best.metadata.get('seqno'), 'timestamp': best.timestamp}
                # Decision: infer IST-only vs SST(+IST) based on signals near the request
                # If we have strong IST signals (recv_summary/applying_start) within +/-5 minutes of request
                # and no real SST started (only bookkeeping/helper), mark as IST-only
                def within_window(ev, center_dt, seconds):
                    try:
                        dt = ts_to_dt(ev.timestamp)
                        return dt and abs((dt - center_dt).total_seconds()) <= seconds
                    except Exception:
                        return False

                strong_ist_near = any(within_window(ev, req_dt, 300) for ev in ist_recv_summary)
                # Consider 'sst_proceeding' and cleanup/wait markers as strong indicators of real SST
                sst_strong_near = (
                    any(within_window(ev, req_dt, 600) for ev in sst_proceedings) or
                    any(within_window(ev, req_dt, 600) for ev in sst_clean_datadir) or
                    any(within_window(ev, req_dt, 600) for ev in sst_wait_stream) or
                    any(within_window(ev, req_dt, 600) for ev in sst_stream_addrs) or
                    any(within_window(ev, req_dt, 900) for ev in sst_sents) or
                    any(within_window(ev, req_dt, 1800) for ev in sst_total_time_donor) or
                    any(within_window(ev, req_dt, 900) for ev in sst_complete_notices)
                )
                helper_ist_near = any(within_window(ev, req_dt, 300) for ev in sst_helper_ist)
                bookkeeping_near = any(within_window(ev, req_dt, 300) for ev in sst_bookkeeping)

                # If we detect an SST failure near this request, mark as SST FAILED and block IST
                block_post_ist = False
                if sst_fail_ev and (not sst_comp_ev or (sst_fail_dt and sst_start_dt and sst_fail_dt <= sst_comp_dt if sst_comp_dt else True)):
                    wf['decision'] = 'SST FAILED'
                    block_post_ist = True
                elif strong_ist_near and (helper_ist_near or bookkeeping_near) and not sst_strong_near:
                    wf['decision'] = 'IST'
                    wf['sst']['note'] = 'SST not needed'
                elif sst_strong_near:
                    # Initially mark as SST; we'll promote to SST+IST only if we actually observe IST after SST
                    wf['decision'] = 'SST'
                else:
                    # Fallback: if we later see async IST starts without SST start, call it IST
                    future_async = any(ev for ev in ist_async_start if within_window(ev, req_dt, 900))
                    if future_async and not sst_strong_near:
                        wf['decision'] = 'IST'
                    else:
                        if wf.get('sst') and wf['sst'].get('status'):
                            status = str(wf['sst'].get('status')).lower()
                            # Map status to readable decision
                            if status == 'started':
                                wf['decision'] = 'SST'
                            elif status == 'failed':
                                wf['decision'] = 'SST FAILED'
                            elif status == 'completed':
                                wf['decision'] = 'SST'
                            else:
                                wf['decision'] = status.upper()
                        else:
                            wf['decision'] = 'Unknown'

                # Allow IST after SST only if SST completed (strongly) or we're in IST-only decision
                # Anchor IST correlation to SST completion time when available
                sst_done_dt = None
                if sst_comp_dt:
                    sst_done_dt = sst_comp_dt
                else:
                    # Look for provider completion notices near the request
                    try:
                        notice_dt = None
                        for ev in sst_complete_notices:
                            dt = ts_to_dt(ev.timestamp)
                            if dt and sst_start_dt and 0 <= (dt - sst_start_dt).total_seconds() <= 1800:
                                if notice_dt is None or dt < notice_dt:
                                    notice_dt = dt
                        if notice_dt:
                            sst_done_dt = notice_dt
                    except Exception:
                        pass

                allow_post_ist = (wf.get('decision') == 'IST') or (sst_done_dt is not None)

                # Post-SST IST async start and completion within windows; assign each IST event only once
                post_start = None
                post_start_dt = None
                if not block_post_ist and allow_post_ist:
                    for ev in ist_async_start:
                        try:
                            ev_dt = ts_to_dt(ev.timestamp)
                            # Anchor post-IST to SST completion when available; else to request for IST-only
                            anchor_dt = sst_done_dt or (req_dt if wf.get('decision') == 'IST' else None)
                            if not anchor_dt:
                                continue
                            if ev_dt and 0 <= (ev_dt - anchor_dt).total_seconds() <= 600 and id(ev) not in used_async_ids:
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
                if not block_post_ist and allow_post_ist:
                    for ev in (ist_completed + ist_async_served + [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_completed_applier']):
                        try:
                            ev_dt = ts_to_dt(ev.timestamp)
                            anchor_dt = sst_done_dt or (req_dt if wf.get('decision') == 'IST' else None)
                            if not anchor_dt:
                                continue
                            if ev_dt and 0 <= (ev_dt - anchor_dt).total_seconds() <= 900 and id(ev) not in used_completed_ids:
                                if post_comp_dt is None or ev_dt < post_comp_dt:
                                    post_comp = ev
                                    post_comp_dt = ev_dt
                        except Exception:
                            pass
                if post_comp:
                    used_completed_ids.add(id(post_comp))
                    wf['post_ist']['completed_at'] = post_comp.timestamp

                # Joiner-side post-IST signals: only if allowed (after SST completion or IST-only)
                # Use sst_done_dt as boundary if available; otherwise use req_dt only for IST-only workflows
                center_dt = (sst_done_dt if allow_post_ist and sst_done_dt else (req_dt if wf.get('decision') == 'IST' else None))
                # Receiver applying-start
                try:
                    receiver_apply = None
                    receiver_apply_dt = None
                    for ev in [e for e in ist_events if e.metadata and e.metadata.get('subtype') == 'ist_applying_start']:
                        dt = ts_to_dt(ev.timestamp)
                        if not dt or not center_dt:
                            continue
                        if 0 <= (dt - center_dt).total_seconds() <= 900:
                            if receiver_apply_dt is None or dt < receiver_apply_dt:
                                receiver_apply = ev
                                receiver_apply_dt = dt
                    if receiver_apply:
                        wf['post_ist']['receiver_applying_start'] = {
                            'timestamp': receiver_apply.timestamp,
                            'first_seqno': receiver_apply.metadata.get('first_seqno')
                        }
                except Exception:
                    pass
                # Receiver range summary (Receiving IST: seqnos A-B) if present
                try:
                    receiver_summary = None
                    receiver_summary_dt = None
                    for ev in [e for e in ist_events if e.metadata and e.metadata.get('subtype') in ('ist_recv_summary','ist_uuid_range')]:
                        dt = ts_to_dt(ev.timestamp)
                        if not dt or not center_dt:
                            continue
                        if 0 <= (dt - center_dt).total_seconds() <= 900:
                            if receiver_summary_dt is None or dt < receiver_summary_dt:
                                receiver_summary = ev
                                receiver_summary_dt = dt
                    if receiver_summary:
                        wf['post_ist']['receiver_range'] = {
                            'first_seqno': receiver_summary.metadata.get('first_seqno'),
                            'last_seqno': receiver_summary.metadata.get('last_seqno'),
                            'timestamp': receiver_summary.timestamp
                        }
                except Exception:
                    pass
                # If we observed IST after SST completion, promote decision to SST+IST
                if wf.get('decision') == 'SST' and wf.get('post_ist') and any(k in wf['post_ist'] for k in ('async_start','receiver_applying_start','receiver_range','completed_at')):
                    wf['decision'] = 'SST+IST'
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
                # Accurate counts derived from view membership
                "total_nodes": self._compute_total_nodes(),
                "active_nodes": self._compute_active_nodes(),
                "current_view": asdict(self.cluster.current_view) if self.cluster.current_view else None,
                "total_views": len(self.cluster.views_history),
                "cluster_name": self.cluster.name,
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
                "mariadb_full_banner": self.software.get('mariadb_full_banner'),
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
            "detailed_events": [
                {
                    "timestamp": event.timestamp,
                    "event_type": event.event_type.value,
                    "raw_message": event.raw_message,
                    "state_transition": {
                        "from_state": event.state_transition.from_state,
                        "to_state": event.state_transition.to_state,
                        "sequence_number": event.state_transition.sequence_number
                    } if event.state_transition else None,
                    "cluster_view": {
                        "view_id": event.cluster_view.view_id,
                        "status": event.cluster_view.status,
                        "members": {uuid: asdict(member) for uuid, member in event.cluster_view.members.items()} if event.cluster_view.members else {}
                    } if event.cluster_view else None,
                    "node": asdict(event.node) if event.node else None,
                    "metadata": event.metadata
                }
                for event in self.events
            ],
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

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp string to datetime object"""
        try:
            return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return None

    def _format_duration(self, seconds: float) -> str:
        """Format duration in seconds to human-readable string"""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.1f}h"
        else:
            days = seconds / 86400
            return f"{days:.1f}d"

    def _analyze_downtime_periods(self) -> List[DowntimePeriod]:
        """Analyze log events to identify downtime periods and gaps"""
        downtime_periods = []
        
        # Get all cluster views and state transitions sorted by timestamp
        views = self.get_cluster_views()
        transitions = self.get_state_transitions()
        
        # Get all events (for activity detection)
        all_events = sorted([e for e in self.events if e.timestamp], 
                           key=lambda x: self._parse_timestamp(x.timestamp) or datetime.min)
        
        if len(all_events) < 2:
            return downtime_periods
        
        # Look for activity gaps (periods with no log events at all)
        downtime_periods.extend(self._detect_activity_gaps(all_events))
        
        # Look for specific patterns indicating downtime
        downtime_periods.extend(self._detect_no_primary_periods(views))
        downtime_periods.extend(self._detect_cluster_shutdown_periods(transitions))
        
        # Sort by start time and merge overlapping periods
        downtime_periods.sort(key=lambda x: x.start_time)
        downtime_periods = self._merge_overlapping_periods(downtime_periods)
        
        return downtime_periods

    def _detect_activity_gaps(self, all_events: List) -> List[DowntimePeriod]:
        """Detect periods with no log activity at all (true silence)"""
        activity_gaps = []
        
        for i in range(len(all_events) - 1):
            current_event = all_events[i]
            next_event = all_events[i + 1]
            
            current_dt = self._parse_timestamp(current_event.timestamp)
            next_dt = self._parse_timestamp(next_event.timestamp)
            
            if not current_dt or not next_dt:
                continue
                
            gap_seconds = (next_dt - current_dt).total_seconds()
            
            # Only consider significant gaps with no activity (>= 5 minutes)
            if gap_seconds >= 300:
                # Check if this gap represents actual downtime or just recovery activity
                # Skip gaps if they're surrounded by recovery-related events
                if self._is_recovery_period(current_event, next_event):
                    continue
                    
                activity_gaps.append(DowntimePeriod(
                    start_time=current_event.timestamp,
                    end_time=next_event.timestamp,
                    duration_seconds=gap_seconds,
                    duration_str=self._format_duration(gap_seconds),
                    gap_type="no_activity",
                    description=f"No log activity for {self._format_duration(gap_seconds)}",
                    severity="high" if gap_seconds > 1800 else "medium"
                ))
        
        return activity_gaps

    def _is_recovery_period(self, before_event, after_event) -> bool:
        """Check if a gap is during a recovery period (SST, IST, reconnection attempts)"""
        # Check event types to see if we're in a recovery scenario
        recovery_types = {EventType.SST_EVENT, EventType.IST_EVENT, EventType.STATE_TRANSITION}
        
        if (before_event.event_type in recovery_types or 
            after_event.event_type in recovery_types):
            return True
            
        # Check for specific recovery-related patterns in the messages
        recovery_keywords = [
            'SST', 'IST', 'state transfer', 'requesting state transfer',
            'JOINER', 'DONOR', 'joining', 'syncing', 'recovery', 
            'reconnect', 'connection failed', 'broken pipe'
        ]
        
        for keyword in recovery_keywords:
            if (keyword.lower() in before_event.raw_message.lower() or
                keyword.lower() in after_event.raw_message.lower()):
                return True
                
        return False

    def _detect_cluster_shutdown_periods(self, transitions: List[StateTransition]) -> List[DowntimePeriod]:
        """Detect periods where nodes are shutting down/destroyed"""
        shutdown_periods = []
        
        for transition in transitions:
            if transition.to_state.upper() in ['CLOSED', 'DESTROYING', 'DESTROYED']:
                # Look for the corresponding restart
                restart_time = None
                transition_dt = self._parse_timestamp(transition.timestamp)
                if not transition_dt:
                    continue
                    
                # Look for next startup within reasonable time (up to 1 hour)
                for later_transition in transitions:
                    later_dt = self._parse_timestamp(later_transition.timestamp)
                    if (later_dt and later_dt > transition_dt and
                        later_transition.from_state.upper() in ['CLOSED', 'OPEN'] and
                        later_transition.to_state.upper() in ['OPEN', 'PRIMARY', 'JOINER']):
                        
                        gap_seconds = (later_dt - transition_dt).total_seconds()
                        if gap_seconds <= 3600:  # Within 1 hour
                            restart_time = later_transition.timestamp
                            break
                
                if restart_time:
                    restart_dt = self._parse_timestamp(restart_time)
                    if restart_dt:
                        gap_seconds = (restart_dt - transition_dt).total_seconds()
                        if gap_seconds > 60:  # At least 1 minute
                            shutdown_periods.append(DowntimePeriod(
                                start_time=transition.timestamp,
                                end_time=restart_time,
                                duration_seconds=gap_seconds,
                                duration_str=self._format_duration(gap_seconds),
                                gap_type="node_restart",
                                description=f"Node shutdown/restart ({self._format_duration(gap_seconds)})",
                                severity="medium" if gap_seconds < 600 else "high"
                            ))
        
        return shutdown_periods

    def _detect_no_primary_periods(self, views: List[ClusterView]) -> List[DowntimePeriod]:
        """Detect periods where cluster had no primary component"""
        no_primary_periods = []
        
        non_primary_start = None
        for view in views:
            if view.status != 'primary':
                if non_primary_start is None:
                    non_primary_start = view
            else:  # primary view
                if non_primary_start is not None:
                    # End of non-primary period
                    start_dt = self._parse_timestamp(non_primary_start.timestamp)
                    end_dt = self._parse_timestamp(view.timestamp)
                    
                    if start_dt and end_dt:
                        gap_seconds = (end_dt - start_dt).total_seconds()
                        if gap_seconds > 30:  # Significant non-primary period
                            no_primary_periods.append(DowntimePeriod(
                                start_time=non_primary_start.timestamp,
                                end_time=view.timestamp,
                                duration_seconds=gap_seconds,
                                duration_str=self._format_duration(gap_seconds),
                                gap_type="no_primary",
                                description=f"Cluster in non-primary state ({self._format_duration(gap_seconds)})",
                                severity="critical" if gap_seconds > 300 else "high"
                            ))
                    
                    non_primary_start = None
        
        return no_primary_periods

    def _merge_overlapping_periods(self, periods: List[DowntimePeriod]) -> List[DowntimePeriod]:
        """Merge overlapping or adjacent downtime periods"""
        if not periods:
            return periods
        
        merged = []
        current = periods[0]
        
        for next_period in periods[1:]:
            current_end = self._parse_timestamp(current.end_time)
            next_start = self._parse_timestamp(next_period.start_time)
            
            if current_end and next_start and (next_start - current_end).total_seconds() <= 60:
                # Merge periods if they're within 1 minute of each other
                next_end = self._parse_timestamp(next_period.end_time)
                current_start = self._parse_timestamp(current.start_time)
                if next_end and current_start:
                    gap_seconds = (next_end - current_start).total_seconds()
                    current = DowntimePeriod(
                        start_time=current.start_time,
                        end_time=next_period.end_time,
                        duration_seconds=gap_seconds,
                        duration_str=self._format_duration(gap_seconds),
                        gap_type="merged",
                        description=f"Combined downtime period ({self._format_duration(gap_seconds)})",
                        severity=max(current.severity, next_period.severity, key=lambda x: ['low', 'medium', 'high', 'critical'].index(x))
                    )
            else:
                merged.append(current)
                current = next_period
        
        merged.append(current)
        return merged

    def analyze_downtime(self) -> None:
        """Analyze and store downtime periods in health metrics"""
        self.health_metrics.downtime_periods = self._analyze_downtime_periods()
        self.health_metrics.total_downtime_seconds = sum(
            period.duration_seconds for period in self.health_metrics.downtime_periods
        )

    # Node counting helpers (reintroduced)
    def _compute_total_nodes(self) -> int:
    # TODO(improvement): Consolidate sequential restart placeholder names (Local-<hex>)
    # that never appear concurrently into a single physical node so Total Nodes reflects
    # logical members rather than per-restart instance identifiers. Heuristic plan:
    # 1) Track timeline of primary views; 2) Detect pattern where exactly one member
    #    name changes among Local-* variants while its UUID changes; 3) Map all such
    #    Local-* names to the longest-lived (or first) and exclude duplicates from count.
    # 4) Preserve raw list elsewhere for forensic detail.
        names = set()
        uuids_without_name = set()
        for view in self.cluster.views_history:
            for node in view.members.values():
                if not node:
                    continue
                if node.name and not node.name.startswith('Temp-') and node.name != 'N/A':
                    names.add(node.name)
                elif node.uuid:
                    uuids_without_name.add(node.uuid)
        if not names and uuids_without_name:
            return len(uuids_without_name)
        return len(names)

    def _compute_active_nodes(self) -> int:
        for view in reversed(self.cluster.views_history):
            if view.status.lower() == 'primary':
                return len(view.members)
        if self.cluster.current_view and self.cluster.current_view.status.lower() == 'primary':
            return len(self.cluster.current_view.members)
        return 0

def output_text(analyzer: GaleraLogAnalyzer) -> str:
    """Generate human-readable text output"""
    lines = []
    # Build summary first so we can show dialect in the header banner
    summary = analyzer.get_cluster_summary()
    cluster_info = summary["cluster_info"]
    software = summary.get("software", {})

    # Header banner
    lines.append("="*70)
    lines.append("G R A M B O - GALERA CLUSTER SINGLE NODE LOG ANALYZER")
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
    if cluster_info.get('cluster_name'):
        lines.append(f"Cluster Name: {cluster_info.get('cluster_name')}")
    # Software versions
    if software:
        full_banner = software.get('mariadb_full_banner')
        mver = software.get('mariadb_version')
        med = software.get('mariadb_edition')
        gver = software.get('galera_version')
        gvar = software.get('galera_variant')
        gvend = software.get('galera_vendor')
        # Display preference: full banner from log if available; else numeric version; else unknown
        if full_banner:
            lines.append(f"MariaDB: {full_banner}")
        else:
            if mver:
                if med:
                    lines.append(f"MariaDB: {mver} ({med})")
                else:
                    lines.append(f"MariaDB: {mver}")
            else:
                lines.append("MariaDB: unknown")
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
    
    # Node Details - show only physical nodes with a single consolidated entry
    lines.append("\n🖥️  CLUSTER NODES (UUID changes upon restart, see section below)")
    lines.append("-" * 50)
    # Build mapping physical_name -> list of node objects
    phys_map: Dict[str, List[Node]] = {}
    for n in analyzer.cluster.nodes.values():
        if not n.name or n.name in ['offset']:
            continue
        # Treat these as transient / restart-scoped identities; skip for primary list
        transient = (
            n.name.startswith('Temp-') or n.name.startswith('Node-') or
            n.name.startswith('node_') or n.name.startswith('Local-')
        )
        if transient:
            continue
        phys_map.setdefault(n.name, []).append(n)

    # Helper to choose a deterministic final UUID: pick the last chronological full UUID across histories
    def pick_final_uuid(nodes: List[Node]) -> Optional[str]:
        # Collect (uuid, first_seen_index) preserving order of appearance inferred from history order
        ordered: List[str] = []
        for nd in nodes:
            # Build combined sequence: historical then current, maintaining original order
            seq = []
            if nd.uuid_history:
                seq.extend(nd.uuid_history)
            if nd.uuid and nd.uuid not in seq:
                seq.append(nd.uuid)
            for u in seq:
                if not u or u in ordered:
                    continue
                if u.count('-') == 4 and len(u) >= 36:
                    ordered.append(u)
        if not ordered:
            return None
        return ordered[-1]  # Last observed full UUID

    if phys_map:
        for pname, plist in phys_map.items():
            final_uuid = pick_final_uuid(plist)
            addr = None
            # Prefer a non-placeholder address if available
            for cand in plist:
                if cand.address and cand.address != 'ip:unknown':
                    addr = cand.address
            addr = addr or 'ip:unknown'
            if final_uuid:
                lines.append(f"• {pname} (UUID: {final_uuid}) [{addr}]")
            else:
                lines.append(f"• {pname} (UUID: <not possible to determine>) [{addr}]")
    else:
        lines.append("• No distinct cluster nodes identified")

    # UUID history section directly below
    lines.append("\n🗂️  CLUSTER NODES UUID HISTORY")
    lines.append("-" * 50)
    for pname, plist in phys_map.items():
        # We need to recompute a final_uuid here for suffix logic (mirror earlier selection)
        recomputed_final = pick_final_uuid(plist)
        ordered_full: List[str] = []
        for nd in plist:
            seq = []
            if nd.uuid_history:
                seq.extend(nd.uuid_history)
            if nd.uuid and nd.uuid not in seq:
                seq.append(nd.uuid)
            for u in seq:
                if u and u.count('-') == 4 and len(u) >= 36 and u not in ordered_full:
                    ordered_full.append(u)
        # Candidate transient UUID collection for local node
        if not ordered_full and cluster_info.get('local_node_name') == pname:
            candidate_full: List[str] = []
            for n in analyzer.cluster.nodes.values():
                nm = n.name or ''
                if nm.startswith('Local-') or nm.startswith('Temp-') or nm.startswith('Node-'):
                    seq2 = []
                    if n.uuid_history:
                        seq2.extend(n.uuid_history)
                    if n.uuid and n.uuid not in seq2:
                        seq2.append(n.uuid)
                    for u2 in seq2:
                        if u2 and u2.count('-') == 4 and len(u2) >= 36 and u2 not in candidate_full:
                            candidate_full.append(u2)
            if candidate_full:
                ordered_full = candidate_full
        # Fallback to raw listening candidates (more permissive)
        if not ordered_full and analyzer.cluster.candidate_local_uuids:
            local_name = cluster_info.get('local_node_name')
            sole_physical = len(phys_map) == 1
            target_name = local_name
            if not target_name:
                # Heuristic: choose name containing '01' else first alphabetically
                for candidate_name in sorted(phys_map.keys()):
                    if re.search(r'(?:^|[^0-9])01(?:[^0-9]|$)', candidate_name):
                        target_name = candidate_name
                        break
                if not target_name and phys_map:
                    target_name = sorted(phys_map.keys())[0]
                if target_name:
                    # We are in rendering context; 'analyzer' holds the instance
                    analyzer.cluster.local_node_name = target_name
            if target_name == pname or sole_physical:
                seen_raw = set()
                ordered_full = []
                for u in analyzer.cluster.candidate_local_uuids:
                    if u in seen_raw:
                        continue
                    seen_raw.add(u)
                    ordered_full.append(u)
        # Nothing found
        if not ordered_full:
            lines.append(f"• {pname} UUIDs <not possible to determine>")
            continue
        # Build compact list
        seen_short: Set[str] = set()
        compacts: List[str] = []
        for fu in ordered_full:
            if fu.count('-') >= 4 and len(fu) >= 36:
                parts = fu.split('-')
                short = f"{parts[0]}-{parts[3][:4]}"
            else:
                # Partial/short form: display as-is (use full captured short form)
                short = fu
            if short in seen_short:
                continue
            seen_short.add(short)
            compacts.append(short)
        local_name = cluster_info.get('local_node_name')
        suffix = " (candidates)" if ((local_name == pname) or (not local_name and len(phys_map)==1)) and recomputed_final is None else ""
        lines.append(f"• {pname} UUIDs {', '.join(compacts)}{suffix}")
    
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
            # Expand member list with names and short UUIDs if available
            try:
                if view.members:
                    # Collect display strings
                    member_displays = []
                    for uuid, node in view.members.items():
                        raw_name = getattr(node, 'name', None) or f"uuid:{uuid[:8]}"
                        name = 'N/A' if raw_name.startswith('Temp-') else raw_name
                        # Short display: first segment (8) + '-' + first 4 chars of 4th segment if available
                        short_uuid = uuid
                        if uuid and len(uuid) >= 36 and uuid.count('-') == 4:
                            parts = uuid.split('-')
                            short_uuid = f"{parts[0]}-{parts[3][:4]}"
                        elif uuid and '-' in uuid:
                            # Fallback for short/composite existing forms
                            segs = uuid.split('-')
                            if len(segs) >= 2:
                                short_uuid = f"{segs[0]}-{segs[1][:4]}"
                        else:
                            short_uuid = uuid[:8]
                        member_displays.append(f"{name}({short_uuid})")
                    # Wrap line length sensibly
                    if member_displays:
                        joined = ", ".join(member_displays)
                        if len(joined) <= 110:
                            lines.append(f"     → {joined}")
                        else:
                            # Break into multiple lines ~100 chars
                            current = []
                            current_len = 0
                            for part in member_displays:
                                if current_len + len(part) + (2 if current else 0) > 100:
                                    lines.append(f"     → {' ,'.replace(' ','') if False else ''}" + ", ".join(current))
                                    current = [part]
                                    current_len = len(part)
                                else:
                                    current.append(part)
                                    current_len += len(part) + (2 if current_len else 0)
                            if current:
                                lines.append(f"     → {', '.join(current)}")
            except Exception:
                pass
    
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
    
    # Downtime Analysis
    if analyzer.health_metrics.downtime_periods:
        lines.append("\n⏰ DOWNTIME ANALYSIS")
        lines.append("-" * 50)
        total_downtime = analyzer.health_metrics.total_downtime_seconds
        lines.append(f"Total Downtime: {analyzer._format_duration(total_downtime)}")
        lines.append(f"Downtime Periods: {len(analyzer.health_metrics.downtime_periods)}")
        lines.append("")
        
        # Group by severity for better organization
        by_severity = {'critical': [], 'high': [], 'medium': [], 'low': []}
        for period in analyzer.health_metrics.downtime_periods:
            by_severity[period.severity].append(period)
        
        # Display periods by severity (critical first)
        for severity in ['critical', 'high', 'medium', 'low']:
            periods = by_severity[severity]
            if periods:
                severity_icon = {'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🟢'}[severity]
                lines.append(f"{severity_icon} {severity.upper()} ({len(periods)} periods):")
                
                for period in sorted(periods, key=lambda x: x.start_time):
                    lines.append(f"  {period.start_time} → {period.end_time}")
                    lines.append(f"    Duration: {period.duration_str} | Type: {period.gap_type}")
                    lines.append(f"    {period.description}")
                    lines.append("")
    
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
            sst_completions = [e for e in sst_events if e.metadata and e.metadata.get('subtype') == 'sst_completed']

            # Helper to find nearest event after a given timestamp within a window
            def _ts_to_dt(ts: str):
                try:
                    return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    return None

            req_list = sst_requests[-5:]
            # Track which START events are already consumed by a prior request block
            used_start_ids: Set[int] = set()
            for idx, req in enumerate(req_list):
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
                    # First, determine failure time bound (if any) to constrain START selection
                    nearest_fail = None
                    nearest_fail_dt = None
                    for fl in sst_failures:
                        fl_dt = _ts_to_dt(fl.timestamp)
                        if fl_dt and 0 <= (fl_dt - req_dt).total_seconds() <= 900:
                            if not nearest_fail_dt or fl_dt < nearest_fail_dt:
                                nearest_fail = fl
                                nearest_fail_dt = fl_dt
                    # Select START within a small pre-window and up to failure (if present), not reusing previous starts
                    best_delta = None
                    for st in sst_starts:
                        if id(st) in used_start_ids:
                            continue
                        st_dt = _ts_to_dt(st.timestamp)
                        if not st_dt:
                            continue
                        # Allow START slightly before request (up to 15s) or after (up to 600s)
                        delta_s = (st_dt - req_dt).total_seconds()
                        if delta_s < -15 or delta_s > 600:
                            continue
                        # Ensure START isn't after failure for this same request
                        if nearest_fail_dt and st_dt > nearest_fail_dt:
                            continue
                        d = abs(delta_s)
                        if best_delta is None or d < best_delta:
                            best_delta = d
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

                # Also show the first failure following the request
                # (recomputed above if req_dt set)
                if 'nearest_fail' not in locals():
                    nearest_fail = None
                    nearest_fail_dt = None
                    if req_dt:
                        for fl in sst_failures:
                            fl_dt = _ts_to_dt(fl.timestamp)
                            if fl_dt and 0 <= (fl_dt - req_dt).total_seconds() <= 900:
                                if not nearest_fail_dt or fl_dt < nearest_fail_dt:
                                    nearest_fail = fl
                                    nearest_fail_dt = fl_dt
                # Build chronological sst_progress entries for this request
                sst_progress = []
                # Include STARTED only if it does not occur after a failure in the same request window
                include_start = bool(nearest_start and nearest_start_dt and (not nearest_fail_dt or nearest_start_dt <= nearest_fail_dt))
                if include_start and nearest_start:
                    sst_progress.append((nearest_start_dt, f"  {nearest_start.timestamp} | SST STARTED"))
                    used_start_ids.add(id(nearest_start))
                if nearest_fail and nearest_fail_dt:
                    reason = (nearest_fail.metadata or {}).get('reason')
                    op = (nearest_fail.metadata or {}).get('operation')
                    code = (nearest_fail.metadata or {}).get('error_code')
                    extra_bits = []
                    if reason:
                        extra_bits.append(reason)
                    if op:
                        extra_bits.append(op)
                    if code:
                        extra_bits.append(str(code))
                    extra = (" (" + ", ".join(extra_bits) + ")") if extra_bits else ""
                    sst_progress.append((nearest_fail_dt, f"  {nearest_fail.timestamp} | SST FAILED{extra}"))
                for _, msg in sorted(sst_progress, key=lambda x: x[0]):
                    lines.append(msg)

                # If a completion happened after the request (and optionally after start), show it
                nearest_complete = None
                nearest_complete_dt = None
                if req_dt and sst_completions:
                    for ev in sst_completions:
                        cdt = _ts_to_dt(ev.timestamp)
                        if cdt and 0 <= (cdt - req_dt).total_seconds() <= 1800:
                            # If start is present, prefer completions after start
                            if nearest_start_dt and cdt < nearest_start_dt:
                                continue
                            if not nearest_complete_dt or cdt < nearest_complete_dt:
                                nearest_complete = ev
                                nearest_complete_dt = cdt
                if nearest_complete:
                    lines.append(f"  {nearest_complete.timestamp} | SST COMPLETED")

                # Visual spacing between attempts
                if idx < len(req_list) - 1:
                    lines.append("")

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
                for cm in sst_completions[-3:]:
                    lines.append(f"  {cm.timestamp} | SST COMPLETED")

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
                raw_decision = (wf.get('decision') or '').upper()
                # Normalize display to SST, IST, or SST+IST only
                if raw_decision in ('SST+IST', 'IST', 'SST'):
                    display_process = raw_decision
                elif raw_decision == 'SST FAILED':
                    display_process = 'SST'
                else:
                    # Heuristic fallback: if SST block exists, show SST; else if any post_ist keys, show IST; else SST
                    display_process = 'SST' if wf.get('sst') else ('IST' if wf.get('post_ist') else 'SST')
                lines.append(f"Request {wf.get('requested_at')}: {joiner} ⇐ {donor} | Process: {display_process}")
                # Show donor Galera coordinates if available
                if wf.get('galera_coords'):
                    gc = wf['galera_coords']
                    lines.append(f"  Donor co-ords: {gc.get('uuid')}:{gc.get('seqno')} at {gc.get('timestamp')}")
                pre = wf.get('pre_ist_signals', [])
                if pre:
                    kinds = ", ".join(p.get('type', '') for p in pre)
                    lines.append(f"  Pre-IST: {kinds}")
                sst = wf.get('sst', {})
                if sst:
                    start_ts = sst.get('timestamp')
                    comp_ts = sst.get('completed_at')
                    note = sst.get('note')
                    if start_ts:
                        lines.append(f"  SST: started at {start_ts}" + (f" ({note})" if note else ""))
                    if comp_ts:
                        lines.append(f"  SST: completed at {comp_ts}")
                post = wf.get('post_ist', {})
                if post:
                    if 'async_start' in post:
                        a = post['async_start']
                        # Address and match indicator
                        addr = wf.get('joiner_addr')
                        match = post.get('peer_matched')
                        match_str = ' ✓' if match else (' ×' if match is False else '')
                        if addr:
                            lines.append(f"  IST: async serve {a.get('peer')} {a.get('first_seqno')}→{a.get('last_seqno')} at {a.get('timestamp')} (joiner {addr}{match_str})")
                        else:
                            lines.append(f"  IST: async serve {a.get('peer')} {a.get('first_seqno')}→{a.get('last_seqno')} at {a.get('timestamp')}{match_str}")
                    if 'receiver_applying_start' in post:
                        ra = post['receiver_applying_start']
                        lines.append(f"  IST: applying starts at {ra.get('first_seqno')} ({ra.get('timestamp')})")
                    if 'receiver_range' in post:
                        rr = post['receiver_range']
                        lines.append(f"  IST: receiving {rr.get('first_seqno')}→{rr.get('last_seqno')} at {rr.get('timestamp')}")
                    if 'completed_at' in post:
                        lines.append(f"  IST: completed at {post['completed_at']}")
    
    # Recent Critical Events: include only truly critical items
    def _is_deprecation_warning(msg: str) -> bool:
        s = msg.lower()
        return 'deprecated' in s and 'wsrep' in s

    critical_list = []
    for e in analyzer.events:
        raw = (e.raw_message or '')
        low = raw.lower()
        meta = e.metadata or {}
        node_name = e.node.name if e.node else "Unknown"

        # Always include hard errors
        if e.event_type == EventType.ERROR:
            critical_list.append((e.timestamp, node_name, 'ERROR', raw, meta))
            continue

        # Warnings: include only if they indicate failure/abort, exclude deprecations
        if e.event_type == EventType.WARNING:
            if _is_deprecation_warning(raw):
                continue
            if any(k in low for k in ['failed', 'abort', 'cannot', 'refused', 'rejected', 'will never receive state']):
                critical_list.append((e.timestamp, node_name, 'WARNING', raw, meta))
            continue

        # SST failures and rejects
        if e.event_type == EventType.SST_EVENT:
            st = meta.get('subtype')
            if st in {'sst_failed', 'sst_reject', 'sst_reject_wrong_state'}:
                critical_list.append((e.timestamp, node_name, 'SST FAILURE', raw, meta))
            continue

        # IST failures/incomplete
        if e.event_type == EventType.IST_EVENT:
            st = meta.get('subtype')
            if st in {'ist_send_failed', 'async_ist_failed', 'ist_incomplete'}:
                critical_list.append((e.timestamp, node_name, 'IST FAILURE', raw, meta))
            continue

        # Server info critical signals
        if e.event_type == EventType.SERVER_INFO:
            st = meta.get('subtype')
            if st in {'identity_mismatch', 'received_non_primary', 'recv_thread_exit'}:
                label = 'CLUSTER STATE' if st == 'received_non_primary' else 'SERVER'
                critical_list.append((e.timestamp, node_name, label, raw, meta))
            continue

        # Communication issues: include significant failures; skip minor aborted connections
        if e.event_type == EventType.COMMUNICATION_ISSUE:
            st = meta.get('subtype')
            if st and st != 'aborted_connection':
                critical_list.append((e.timestamp, node_name, 'COMMUNICATION', raw, meta))
            else:
                # If pattern suggests serious network issues
                if any(k in low for k in ['failed to connect', 'connection lost', 'network error', 'protocol', 'handshake']):
                    critical_list.append((e.timestamp, node_name, 'COMMUNICATION', raw, meta))

    # Service Events Section
    service_events = [e for e in analyzer.events if e.event_type == EventType.SERVICE_EVENT]
    if service_events:
        lines.append("\n🔧 SERVICE EVENTS")
        lines.append("-" * 50)
        
        # Group by subtype for better organization
        startup_events = [e for e in service_events if e.metadata and e.metadata.get('subtype') == 'startup']
        shutdown_events = [e for e in service_events if e.metadata and e.metadata.get('subtype') == 'shutdown']
        crash_events = [e for e in service_events if e.metadata and e.metadata.get('subtype') == 'crash']
        restart_events = [e for e in service_events if e.metadata and e.metadata.get('subtype') == 'restart']
        
        if startup_events:
            lines.append("📱 Startup Events:")
            for event in startup_events[-5:]:  # Show last 5
                event_type = (event.metadata or {}).get('event', 'startup')
                node_name = event.node.name if event.node else 'Unknown'
                lines.append(f"  {event.timestamp}: [{node_name}] {event_type.replace('_', ' ').title()}")
        
        if shutdown_events:
            lines.append("🛑 Shutdown Events:")
            for event in shutdown_events[-5:]:  # Show last 5
                event_type = (event.metadata or {}).get('event', 'shutdown')
                node_name = event.node.name if event.node else 'Unknown'
                lines.append(f"  {event.timestamp}: [{node_name}] {event_type.replace('_', ' ').title()}")
        
        if crash_events:
            lines.append("💥 Crash/Signal Events:")
            for event in crash_events[-5:]:  # Show last 5
                event_type = (event.metadata or {}).get('event', 'crash')
                signal = (event.metadata or {}).get('signal', '')
                signal_info = f" (Signal {signal})" if signal else ""
                node_name = event.node.name if event.node else 'Unknown'
                lines.append(f"  {event.timestamp}: [{node_name}] {event_type.replace('_', ' ').title()}{signal_info}")
        
        if restart_events:
            lines.append("🔄 Restart Events:")
            for event in restart_events[-5:]:  # Show last 5
                node_name = event.node.name if event.node else 'Unknown'
                lines.append(f"  {event.timestamp}: [{node_name}] Service Restart Detected")

    if critical_list:
        lines.append("\n⚠️  CRITICAL EVENTS")
        lines.append("-" * 50)
        # Show last 10 critical events
        for ts, node_name, label, raw, meta in critical_list[-10:]:
            # Compose concise detail when possible
            detail = None
            st = (meta or {}).get('subtype')
            if st == 'sst_failed':
                reason = meta.get('reason')
                op = meta.get('operation')
                code = meta.get('error_code')
                bits = [b for b in [reason, op, code] if b]
                detail = ", ".join(map(str, bits)) if bits else None
            elif st in {'ist_send_failed', 'async_ist_failed'}:
                detail = meta.get('reason')
            elif st == 'ist_incomplete':
                detail = f"expected {meta.get('expected_last')}, got {meta.get('last_received')}"
            elif st == 'channel_open_failed':
                chan = meta.get('channel'); url = meta.get('url'); code = meta.get('code'); err = meta.get('error')
                detail = f"{chan} {url} ({code} {err})"
            elif st == 'identity_mismatch':
                detail = f"Node UUID {meta.get('node_uuid')} not in view"

            header = f"{ts}: [{node_name}] {label}"
            lines.append(header)
            if detail:
                lines.append(f"   {detail}")
            else:
                lines.append(f"   {raw[:120]}...")
    
    return "\n".join(lines)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Galera Log Analyzer',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('logfile', nargs='?', help='Galera log file to analyze')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--filter', help='Filter events by type (comma-separated)')
    parser.add_argument('--dialect', default='auto', help='(DEPRECATED) Previously forced log dialect; now deduced from MariaDB version/edition. Ignored.')
    parser.add_argument('--report-unknown', action='store_true', help='Report unknown WSREP/IST lines')
    parser.add_argument('--mariadb-version', help='MariaDB server version (e.g., 10.6.16, 11.4.7-4)')
    parser.add_argument('--mariadb-edition', choices=['enterprise', 'community'], help='MariaDB edition')
    parser.add_argument('--galera-version', help='(DEPRECATED) Explicit Galera provider version; inference now automatic when possible. Ignored.')
    
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
    # Handle deprecated flags: warn if explicitly set (not default) and ignore
    deprecated_msgs = []
    if args.dialect and args.dialect != 'auto':
        deprecated_msgs.append(f"--dialect '{args.dialect}' ignored (auto-deduction now based on MariaDB version/edition)")
    if getattr(args, 'galera_version', None):
        deprecated_msgs.append(f"--galera-version '{args.galera_version}' ignored (provider version inferred or parsed)")
    for m in deprecated_msgs:
        print(f"WARNING: {m}", file=sys.stderr)
    analyzer = GaleraLogAnalyzer(
        dialect='auto',  # always auto now
        report_unknown=bool(getattr(args, 'report_unknown', False)),
        mariadb_version=getattr(args, 'mariadb_version', None),
        mariadb_edition=getattr(args, 'mariadb_edition', None),
        galera_version=None,  # ignore deprecated explicit galera version
    )
    analyzer.parse_log(log_lines)

    # Infer Galera from MariaDB if missing
    analyzer.infer_galera_from_mariadb()
    
    # Perform downtime analysis
    analyzer.analyze_downtime()

    # Require MariaDB version known; Galera optional/inferred
    sw = analyzer.software
    have_mariadb = bool(sw.get('mariadb_version'))
    if not have_mariadb:
        # Soft fallback: pick a default dialect (prefer galera generic) and continue
        fallback_dialect = 'galera'
        if analyzer.dialect == 'unknown':
            analyzer.dialect = fallback_dialect
        print("WARNING: MariaDB version/edition not detected or specified; proceeding with default dialect.", file=sys.stderr)
        sw['mariadb_version'] = None  # explicitly None
        sw['mariadb_edition'] = None
    
    # Output results
    if args.format == 'json':
        summary = analyzer.get_cluster_summary()
        print(json.dumps(summary, indent=2, default=str))
    else:
        print(output_text(analyzer))

if __name__ == "__main__":
    main()
