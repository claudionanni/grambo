#!/usr/bin/env python3
"""
GRA-Analyzer: Galera Log Analysis Summary Tool

This script processes the structured output from grap.py and presents it in a clean,
structured view similar to the original gra script but with enhanced temporal entity
information and better organization.

Usage:
    python3 gra-analyzer.py logfile.log
    python3 grap.py --format=json logfile.log | python3 gra-analyzer.py --stdin
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Any, Optional
import subprocess
import re

__version__ = "1.0.0-alpha1"
__author__ = "Claudio Nanni"
__description__ = "Galera Log Analysis Summary Tool - processes grap.py output"


def safe_timestamp_key(obj):
    """Safely extract a sortable timestamp key from an object"""
    timestamp = obj.get('timestamp') if isinstance(obj, dict) else obj
    
    if timestamp is None:
        return ''
    elif isinstance(timestamp, str):
        return timestamp
    elif hasattr(timestamp, 'isoformat'):
        return timestamp.isoformat()
    else:
        return str(timestamp)


class SSTSession:
    """Represents a complete SST session from request to completion"""
    
    def __init__(self, request_time: str, request_line: str):
        self.request_time = request_time
        self.request_line = request_line
        self.donor = None
        self.joiner = None
        self.method = None
        self.status = "ONGOING"  # ONGOING, COMPLETED, FAILED
        self.completion_time = None
        self.completion_line = None
        self.error_message = None
        self.duration_seconds = 0.0  # Duration of the SST session
        self.events = []  # All log events belonging to this session
        
    def add_event(self, timestamp: str, line: str):
        """Add a log event to this session"""
        self.events.append((timestamp, line))
        
        # Extract information from the event
        self._extract_info_from_line(line)
        
    def _extract_info_from_line(self, line: str):
        """Extract donor, joiner, method info from log line"""
        
        # Extract donor and joiner from patterns like:
        # "Selected 1.1 (vinfr-db-d-d01)(SYNCED) as donor"
        # "State transfer from 1.1 (vinfr-db-d-d01) complete"
        # "State transfer to 2.1 (vinfr-db-d-l05) complete"
        
        donor_match = re.search(r'Selected\s+[\d.]+\s+\(([^)]+)\).*as donor', line)
        if donor_match:
            self.donor = donor_match.group(1)
            
        # Extract joiner from request patterns
        joiner_match = re.search(r'Member\s+[\d.]+\s+\(([^)]+)\)\s+requested state transfer', line)
        if joiner_match:
            self.joiner = joiner_match.group(1)
            
        # Extract from completion patterns
        from_donor_match = re.search(r'State transfer from\s+[\d.]+\s+\(([^)]+)\)\s+complete', line)
        if from_donor_match and not self.donor:
            self.donor = from_donor_match.group(1)
            
        to_joiner_match = re.search(r'State transfer to\s+[\d.]+\s+\(([^)]+)\)\s+(?:complete|failed)', line)
        if to_joiner_match and not self.joiner:
            self.joiner = to_joiner_match.group(1)
            
        # Extract method from SST command lines
        if 'wsrep_sst_' in line:
            if 'mariabackup' in line:
                self.method = 'mariabackup'
            elif 'rsync' in line:
                self.method = 'rsync'
            elif 'mysqldump' in line:
                self.method = 'mysqldump'
                
    def mark_completed(self, timestamp: str, line: str):
        """Mark session as completed"""
        self.status = "COMPLETED"
        self.completion_time = timestamp
        self.completion_line = line
        self.add_event(timestamp, line)
        
    def mark_failed(self, timestamp: str, line: str, error_msg: Optional[str] = None):
        """Mark session as failed"""
        self.status = "FAILED"
        self.completion_time = timestamp
        self.completion_line = line
        self.error_message = error_msg
        self.add_event(timestamp, line)
        
    def get_duration_seconds(self) -> float:
        """Calculate session duration in seconds"""
        if not self.completion_time:
            return 0.0
            
        try:
            start_dt = datetime.fromisoformat(self.request_time.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(self.completion_time.replace('Z', '+00:00'))
            return (end_dt - start_dt).total_seconds()
        except:
            return 0.0


class SSTSessionTracker:
    """Tracks SST sessions chronologically from log events"""
    
    def __init__(self, log_file_path: Optional[str] = None):
        self.sessions = []
        self.log_file_path = log_file_path
        
    def process_entities(self, entities: List[Dict]) -> List[SSTSession]:
        """Process entities chronologically to build SST sessions"""
        
        # Use direct log file parsing for more accurate SST detection
        if self.log_file_path:
            return self._build_sessions_from_log_file()
        
        # Fallback to entity processing if no log file
        return self._build_sessions_from_entities(entities)
    
    def _build_sessions_from_log_file(self) -> List[SSTSession]:
        """Build SST sessions by parsing the log file directly"""
        sessions = []
        
        if not self.log_file_path:
            return sessions
        
        try:
            # Try UTF-8 first, fall back to latin-1 if that fails
            try:
                with open(self.log_file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except UnicodeDecodeError:
                with open(self.log_file_path, 'r', encoding='latin-1') as f:
                    lines = f.readlines()
        except (FileNotFoundError, IOError):
            return sessions
        
        current_session = None
        
        for line in lines:
            line = line.strip()
            
            # Extract timestamp from the line
            timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
            if not timestamp_match:
                continue
            timestamp = timestamp_match.group(1)
            
            # Check for SST request patterns
            if 'requested state transfer' in line:
                # If there's an ongoing session, mark it as interrupted/failed
                if current_session:
                    current_session.status = 'INTERRUPTED'
                    current_session.completion_time = timestamp
                    current_session.completion_line = "Session interrupted by new SST request"
                    current_session.add_event(timestamp, "Session interrupted by new SST request")
                    sessions.append(current_session)
                
                # Parse donor and joiner names
                parsed = self._parse_sst_log_line(line)
                
                # Start new session
                current_session = SSTSession(timestamp, line)
                current_session.donor = parsed.get('donor', 'unknown_donor')
                current_session.joiner = parsed.get('joiner', 'unknown_joiner')
                current_session.method = 'mariabackup'  # Default
                current_session.status = 'ONGOING'
                current_session.add_event(timestamp, line)
                
            # Check for SST completion (both success and failure)
            elif current_session and ('Process completed' in line or 'SST completed' in line or 'mariabackup SST completed' in line or 'State transfer' in line and 'failed:' in line):
                # Determine if it was successful or failed
                if 'error' in line.lower() or 'broken pipe' in line.lower() or 'failed:' in line:
                    current_session.status = 'FAILED'
                    # Extract error from different patterns
                    error_match = re.search(r'error:.*?(\d+) \(([^)]+)\)', line)
                    failed_match = re.search(r'failed:\s*(.+)$', line)
                    if error_match:
                        current_session.error_message = f"Exit code {error_match.group(1)}: {error_match.group(2)}"
                    elif failed_match:
                        current_session.error_message = failed_match.group(1).strip()
                    else:
                        current_session.error_message = "Process completed with error"
                else:
                    current_session.status = 'COMPLETED'
                
                current_session.completion_time = timestamp
                current_session.completion_line = line
                current_session.add_event(timestamp, line)
                
                # Calculate duration
                if current_session.request_time and current_session.completion_time:
                    try:
                        start_dt = datetime.fromisoformat(current_session.request_time.replace(' ', 'T'))
                        end_dt = datetime.fromisoformat(current_session.completion_time.replace(' ', 'T'))
                        duration_td = end_dt - start_dt
                        current_session.duration_seconds = duration_td.total_seconds()
                    except:
                        current_session.duration_seconds = 0.0
                
                sessions.append(current_session)
                current_session = None
        
        # Handle any remaining ongoing session
        if current_session:
            current_session.status = 'ONGOING'
            sessions.append(current_session)
        
        return sessions
    
    def _build_sessions_from_entities(self, entities: List[Dict]) -> List[SSTSession]:
        """Fallback method using grap.py entities (original approach)"""
        # First, collect all SST-related log lines from any entity type
        sst_log_lines = self._collect_sst_log_lines(entities)
        
        # Get STATE_TRANSFER entities sorted by timestamp, but filter out problematic ones
        sst_entities = [e for e in entities if e.get('entity_type') == 'STATE_TRANSFER']
        sst_entities = sorted(sst_entities, key=safe_timestamp_key)
        
        # Filter out auto-completed and problematic sessions
        valid_entities = self._filter_valid_sst_entities(sst_entities)
        
        for entity in valid_entities:
            self._convert_entity_to_session(entity, sst_log_lines)
                
        return self.sessions
    
    def _collect_sst_log_lines(self, entities: List[Dict]) -> List[tuple]:
        """Collect all SST-related log lines from all entity types"""
        sst_lines = []
        
        for entity in entities:
            raw_line = entity.get('raw_line', '')
            timestamp = entity.get('timestamp', '')
            
            # Look for SST patterns in the raw line
            if raw_line and any(pattern in raw_line for pattern in [
                'requested state transfer',
                'Selected', 
                'as donor',
                'State transfer to',
                'State transfer from', 
                'complete',
                'failed'
            ]):
                # Parse the line to see if it contains SST info
                parsed = self._parse_sst_log_line(raw_line)
                if parsed:
                    sst_lines.append((timestamp, raw_line, parsed))
        
        return sorted(sst_lines, key=lambda x: x[0] or '')
    
    def _read_sst_lines_from_file(self) -> List[tuple]:
        """Read SST-related lines directly from the log file"""
        sst_lines = []
        
        if not self.log_file_path:
            return sst_lines
            
        try:
            # Try UTF-8 first, fall back to latin-1 if that fails
            try:
                with open(self.log_file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except UnicodeDecodeError:
                with open(self.log_file_path, 'r', encoding='latin-1') as f:
                    lines = f.readlines()
            
            for line in lines:
                    line = line.strip()
                    
                    # Extract timestamp from the line (format: YYYY-MM-DD HH:MM:SS)
                    timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                    timestamp = timestamp_match.group(1) if timestamp_match else ''
                    
                    # Check if line contains SST information
                    if any(pattern in line for pattern in [
                        'requested state transfer',
                        'Selected', 
                        'as donor',
                        'State transfer to',
                        'State transfer from'
                    ]):
                        parsed = self._parse_sst_log_line(line)
                        if parsed:
                            sst_lines.append((timestamp, line, parsed))
                            
        except (FileNotFoundError, IOError) as e:
            # Silently fail if we can't read the file
            pass
            
        return sst_lines
    
    def _filter_valid_sst_entities(self, sst_entities: List[Dict]) -> List[Dict]:
        """Filter out invalid or auto-generated SST entities"""
        valid_entities = []
        
        for entity in sst_entities:
            # Skip auto-completed/interrupted sessions
            status = entity.get('current_transfer_status', '').lower()
            if 'auto_completed' in status or 'interrupted' in status:
                continue
                
            # Skip sessions with negative or extremely long durations
            duration = entity.get('duration_seconds', 0)
            if duration < 0 or duration > 86400:  # More than 24 hours is suspicious
                continue
                
            # Skip sessions where end time is before start time
            start_time = entity.get('start_timestamp', '')
            end_time = entity.get('end_timestamp', '')
            if start_time and end_time:
                try:
                    if start_time > end_time:  # String comparison should work for ISO format
                        continue
                except:
                    pass  # If comparison fails, keep the entity
                    
            valid_entities.append(entity)
            
        return valid_entities
    
    def _convert_entity_to_session(self, entity: Dict, sst_log_lines: List[tuple]):
        """Convert a STATE_TRANSFER entity to an SSTSession"""
        start_time = entity.get('start_timestamp', entity.get('timestamp', ''))
        end_time = entity.get('end_timestamp', '')
        
        # Create session from entity
        session = SSTSession(start_time, entity.get('raw_line', ''))
        
        # Extract better node information
        session.donor = self._extract_node_name(entity, 'donor', sst_log_lines, start_time, end_time)
        session.joiner = self._extract_node_name(entity, 'joiner', sst_log_lines, start_time, end_time)
        session.method = entity.get('transfer_method', 'mariabackup')
        
        # Get duration from the entity (it's already calculated)
        session.duration_seconds = entity.get('duration_seconds', 0.0)
        
        # Determine status from current_transfer_status
        transfer_status = entity.get('current_transfer_status', '').lower()
        if 'completed' in transfer_status:
            session.status = 'COMPLETED'
        elif 'failed' in transfer_status or 'error' in transfer_status:
            session.status = 'FAILED'
            session.error_message = entity.get('current_error_message')
        elif 'started' in transfer_status or 'progress' in transfer_status:
            if end_time:
                # Has end time, so it completed
                session.status = 'COMPLETED'
            else:
                session.status = 'ONGOING'
        else:
            # Unknown status, check if it has an end time
            if end_time:
                session.status = 'COMPLETED' 
            else:
                session.status = 'ONGOING'
                
        # Set completion time and line
        if end_time:
            session.completion_time = end_time
            session.completion_line = f"SST {session.status.lower()} at {end_time}"
            
        # Add some events based on property timeline if available
        property_timeline = entity.get('property_timeline', {})
        if property_timeline:
            # Add start event
            if start_time:
                session.add_event(start_time, f"SST started - {session.method}")
                
            # Add status progression events
            status_timeline = property_timeline.get('transfer_status', [])
            for timestamp, status in status_timeline:
                session.add_event(timestamp, f"SST status: {status}")
                
            # Add completion event
            if end_time:
                session.add_event(end_time, f"SST {session.status.lower()}")
        
        # Try to improve node names by looking through all events in the session
        self._improve_node_names_from_events(session)
        
        self.sessions.append(session)
    
    def _improve_node_names_from_events(self, session: SSTSession):
        """Look through session events to find better node names"""
        for timestamp, event_line in session.events:
            names = self._parse_sst_log_line(event_line)
            if names:
                # Update donor if we found a better name
                if names.get('donor') and (not session.donor or session.donor.startswith('unknown_')):
                    session.donor = names['donor']
                    
                # Update joiner if we found a better name  
                if names.get('joiner') and (not session.joiner or session.joiner.startswith('unknown_')):
                    session.joiner = names['joiner']
    
    def _extract_node_name(self, entity: Dict, role: str, sst_log_lines: List[tuple], start_time: str, end_time: str) -> str:
        """Extract node name with better fallback logic"""
        
        # First, try to find node names in the relevant SST log lines
        def parse_time(ts):
            if not ts:
                return ''
            return ts.replace('T', ' ').split('.')[0] if 'T' in ts else ts
        
        session_start = parse_time(start_time)
        session_end = parse_time(end_time) if end_time else '9999-99-99'
        
        # Look through SST log lines for node names
        for timestamp, raw_line, parsed_info in sst_log_lines:
            line_time = parse_time(timestamp)
            
            # Check if this line is relevant to this session timeframe
            if session_start <= line_time <= session_end:
                if role == 'donor' and parsed_info.get('donor'):
                    return parsed_info['donor']
                elif role == 'joiner' and parsed_info.get('joiner'):
                    return parsed_info['joiner']
        
        # First, try to extract from raw log lines in the entity
        raw_line = entity.get('raw_line', '')
        if raw_line:
            names = self._parse_sst_log_line(raw_line)
            if names:
                if role == 'donor' and names.get('donor'):
                    return names['donor']
                elif role == 'joiner' and names.get('joiner'):
                    return names['joiner']
        
        # Try the direct fields from entity
        if role == 'donor':
            node_name = entity.get('donor_node') or entity.get('donor_address')
        else:  # joiner
            node_name = entity.get('joiner_node') or entity.get('joiner_address')
        
        # If we got a name and it's not empty, clean it up
        if node_name and node_name.strip():
            # Remove quotes if present
            node_name = node_name.strip("'\"")
            
            # If it's an IP:port, try to make it more readable
            if ':' in node_name and node_name.count('.') == 3:
                # It's an IP:port, keep it as is but format nicely
                return f"Node[{node_name}]"
            
            return node_name
        
        # Try to extract from identity_key (format: donor→joiner@timestamp)
        identity_key = entity.get('identity_key', '')
        if identity_key and '→' in identity_key:
            parts = identity_key.split('@')[0]  # Remove timestamp part
            donor_joiner = parts.split('→')
            if len(donor_joiner) == 2:
                if role == 'donor':
                    candidate = donor_joiner[0].strip()
                else:
                    candidate = donor_joiner[1].strip()
                
                if candidate and candidate != 'unknown_donor' and candidate != 'unknown_joiner':
                    # Remove quotes if present
                    candidate = candidate.strip("'\"")
                    return candidate
        
        # Final fallback
        return f'unknown_{role}'
    
    def _parse_sst_log_line(self, line: str) -> Dict[str, str]:
        """Parse SST-related log lines to extract donor and joiner names"""
        result = {}
        
        # Pattern for SST request: Member X.Y (joiner_name) requested state transfer from '*any*'. Selected A.B (donor_name)(SYNCED) as donor.
        sst_request_pattern = r'Member\s+[\d.]+\s+\(([^)]+)\)\s+requested state transfer.*Selected\s+[\d.]+\s+\(([^)]+)\)'
        match = re.search(sst_request_pattern, line, re.IGNORECASE)
        if match:
            result['joiner'] = match.group(1).strip()
            result['donor'] = match.group(2).strip()
            return result
        
        # Pattern for SST completion: State transfer from X.Y (donor_name) complete
        completion_from_pattern = r'State transfer from\s+[\d.]+\s+\(([^)]+)\)\s+complete'
        match = re.search(completion_from_pattern, line, re.IGNORECASE)
        if match:
            result['donor'] = match.group(1).strip()
            return result
            
        # Pattern for SST completion: State transfer to X.Y (joiner_name) complete
        completion_to_pattern = r'State transfer to\s+[\d.]+\s+\(([^)]+)\)\s+(?:complete|failed)'
        match = re.search(completion_to_pattern, line, re.IGNORECASE)
        if match:
            result['joiner'] = match.group(1).strip()
            return result
        
        # Pattern for donor selection: Selected X.Y (donor_name)(SYNCED) as donor
        donor_selection_pattern = r'Selected\s+[\d.]+\s+\(([^)]+)\).*as donor'
        match = re.search(donor_selection_pattern, line, re.IGNORECASE)
        if match:
            result['donor'] = match.group(1).strip()
            return result
            
        return result


class GraAnalyzer:
    """Main analyzer class that processes grap output and creates structured summaries"""
    
    def __init__(self):
        self.entities = []
        self.metadata = {}
        self.analysis_timestamp = datetime.now()
        self.log_file_path = None  # Store the source log file path
        
    def load_from_json(self, json_data: str) -> bool:
        """Load entities from grap JSON output"""
        try:
            data = json.loads(json_data)
            self.entities = data.get('entities', [])
            self.metadata = data.get('metadata', {})
            return True
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}", file=sys.stderr)
            return False
            
    def load_from_grap(self, logfile: Path) -> bool:
        """Run grap.py on logfile and load the JSON output"""
        try:
            # Store the log file path for later use
            self.log_file_path = str(logfile)
            
            cmd = [sys.executable, 'grap.py', '--format=json', str(logfile)]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)
            
            if result.returncode != 0:
                print(f"Error running grap.py: {result.stderr}", file=sys.stderr)
                return False
                
            return self.load_from_json(result.stdout)
        except Exception as e:
            print(f"Error running grap.py: {e}", file=sys.stderr)
            return False
    
    def analyze(self) -> Dict[str, Any]:
        """Perform comprehensive analysis of the entities"""
        analysis = {
            'overview': self._analyze_overview(),
            'nodes': self._analyze_nodes(),
            'cluster_views': self._analyze_cluster_views(),
            'sst_sessions': self._analyze_sst_sessions(),
            'timeline': self._analyze_timeline(),
            'errors': self._analyze_errors(),
            'cluster_state': self._analyze_cluster_state(),
            'performance': self._analyze_performance(),
            'warnings': self._analyze_warnings(),
            'connectivity': self._analyze_connectivity()
        }
        return analysis
    
    def _analyze_overview(self) -> Dict[str, Any]:
        """Generate overview statistics"""
        entity_counts = Counter(entity.get('entity_type', 'unknown') for entity in self.entities)
        
        overview = {
            'total_entities': len(self.entities),
            'entity_types': dict(entity_counts),
            'analysis_time': self.analysis_timestamp.isoformat(),
            'log_timespan': self._get_log_timespan(),
            'grap_metadata': self.metadata
        }
        return overview
    
    def _analyze_sst_sessions(self) -> Dict[str, Any]:
        """Analyze SST sessions in detail"""
        sst_entities = [e for e in self.entities if e.get('entity_type') == 'STATE_TRANSFER']
        
        if not sst_entities:
            return {'total_sessions': 0, 'summary': 'No SST sessions found'}
        
        sessions = {
            'total_sessions': len(sst_entities),
            'by_status': self._group_ssts_by_status(sst_entities),
            'by_method': self._group_ssts_by_method(sst_entities),
            'duration_stats': self._calculate_duration_stats(sst_entities),
            'session_details': self._get_session_details(sst_entities)
        }
        return sessions
    
    def _analyze_timeline(self) -> List[Dict[str, Any]]:
        """Create chronological timeline of events"""
        events = []
        
        for entity in self.entities:
            # Add entity creation event
            if entity.get('start_timestamp') or entity.get('timestamp'):
                timestamp = entity.get('start_timestamp') or entity.get('timestamp')
                events.append({
                    'timestamp': timestamp,
                    'type': 'entity_start',
                    'entity_type': entity.get('entity_type'),
                    'description': self._get_entity_description(entity),
                    'entity_id': entity.get('entity_id'),
                    'details': entity
                })
            
            # Add timeline events from temporal entities
            timeline = entity.get('property_timeline', {})
            for property_name, changes in timeline.items():
                for timestamp, value in changes:
                    events.append({
                        'timestamp': timestamp,
                        'type': 'property_change',
                        'entity_type': entity.get('entity_type'),
                        'property': property_name,
                        'value': value,
                        'entity_id': entity.get('entity_id'),
                        'description': f"{property_name}: {value}"
                    })
        
        # Sort by timestamp (handle both string and datetime objects)
        events.sort(key=safe_timestamp_key)
        return events
    
    def _analyze_errors(self) -> Dict[str, Any]:
        """Analyze errors and warnings"""
        errors = []
        warnings = []
        
        for entity in self.entities:
            # Check for error status
            status = entity.get('current_transfer_status', '')
            if 'fail' in status.lower() or 'error' in status.lower():
                errors.append({
                    'entity_id': entity.get('entity_id'),
                    'type': entity.get('entity_type'),
                    'status': status,
                    'error_message': entity.get('current_error_message', ''),
                    'timestamp': entity.get('start_timestamp') or entity.get('timestamp')
                })
            
            # Check for interrupted or auto-completed sessions
            if 'auto_completed' in status or 'interrupted' in status:
                warnings.append({
                    'entity_id': entity.get('entity_id'),
                    'type': entity.get('entity_type'),
                    'status': status,
                    'message': f"Session was {status}",
                    'timestamp': entity.get('start_timestamp') or entity.get('timestamp')
                })
        
        return {
            'errors': errors,
            'warnings': warnings,
            'error_count': len(errors),
            'warning_count': len(warnings)
        }
    
    def _analyze_cluster_state(self) -> Dict[str, Any]:
        """Analyze cluster state information"""
        cluster_info = {
            'node_roles': self._identify_node_roles(),
            'cluster_membership': self._analyze_cluster_membership(),
            'state_changes': self._track_state_changes()
        }
        return cluster_info
    
    def _analyze_performance(self) -> Dict[str, Any]:
        """Analyze performance metrics"""
        sst_entities = [e for e in self.entities if e.get('entity_type') == 'STATE_TRANSFER']
        
        performance = {
            'sst_success_rate': self._calculate_success_rate(sst_entities),
            'average_duration': self._calculate_average_duration(sst_entities),
            'throughput_stats': self._analyze_throughput(sst_entities)
        }
        return performance
    
    def _analyze_nodes(self) -> Dict[str, Any]:
        """Analyze node information and state"""
        node_entities = [e for e in self.entities if e.get('entity_type') == 'NODE']
        
        nodes = {}
        node_names = set()
        node_ids = set()
        
        for entity in node_entities:
            node_id = entity.get('node_id', '')
            node_name = entity.get('node_name', '')
            
            if node_id:
                node_ids.add(node_id)
            if node_name:
                node_names.add(node_name)
            
            # Create or update node info
            key = node_name or node_id or 'unknown'
            if key not in nodes:
                nodes[key] = {
                    'node_name': node_name,
                    'node_id': node_id,
                    'long_uuid': entity.get('long_uuid', ''),
                    'states': [],
                    'addresses': set(),
                    'first_seen': entity.get('timestamp'),
                    'last_seen': entity.get('timestamp'),
                    'pattern_matches': []
                }
            
            node_info = nodes[key]
            
            # Update node information
            if entity.get('current_state'):
                node_info['states'].append({
                    'timestamp': entity.get('timestamp'),
                    'state': entity.get('current_state'),
                    'previous_state': entity.get('previous_state')
                })
            
            if entity.get('node_address'):
                node_info['addresses'].add(entity.get('node_address'))
            
            if entity.get('timestamp'):
                if not node_info['last_seen'] or entity['timestamp'] > node_info['last_seen']:
                    node_info['last_seen'] = entity['timestamp']
                if not node_info['first_seen'] or entity['timestamp'] < node_info['first_seen']:
                    node_info['first_seen'] = entity['timestamp']
            
            node_info['pattern_matches'].append(entity.get('pattern_name'))
        
        # Convert addresses sets to lists for JSON serialization
        for node_info in nodes.values():
            node_info['addresses'] = list(node_info['addresses'])
        
        return {
            'total_nodes': len(nodes),
            'unique_node_names': list(node_names),
            'unique_node_ids': list(node_ids),
            'node_details': nodes,
            'nodes_with_names': len([n for n in nodes.values() if n['node_name']]),
            'nodes_with_ids': len([n for n in nodes.values() if n['node_id']])
        }
    
    def _analyze_cluster_views(self) -> Dict[str, Any]:
        """Analyze cluster view changes and membership"""
        view_entities = [e for e in self.entities if e.get('entity_type') == 'VIEW']
        
        if not view_entities:
            return {'total_views': 0, 'message': 'No cluster view changes found'}
        
        views = []
        cluster_sizes = []
        
        for entity in view_entities:
            view_info = {
                'timestamp': entity.get('timestamp'),
                'cluster_uuid': entity.get('cluster_uuid', ''),
                'cluster_state': entity.get('cluster_state', ''),
                'members': entity.get('members', []),
                'member_count': len(entity.get('members', [])),
                'member_addresses': entity.get('member_addresses', []),
                'joined_nodes': entity.get('joined_nodes', []),
                'left_nodes': entity.get('left_nodes', []),
                'pattern_name': entity.get('pattern_name'),
                'raw_line': entity.get('raw_line', '')
            }
            views.append(view_info)
            
            if view_info['member_count'] > 0:
                cluster_sizes.append(view_info['member_count'])
        
        views.sort(key=safe_timestamp_key)
        
        return {
            'total_views': len(views),
            'view_changes': views,
            'cluster_size_changes': cluster_sizes,
            'max_cluster_size': max(cluster_sizes) if cluster_sizes else 0,
            'min_cluster_size': min(cluster_sizes) if cluster_sizes else 0,
            'final_cluster_size': cluster_sizes[-1] if cluster_sizes else 0,
            'membership_changes': {
                'total_joins': sum(len(v['joined_nodes']) for v in views),
                'total_leaves': sum(len(v['left_nodes']) for v in views)
            }
        }
    
    def _analyze_warnings(self) -> Dict[str, Any]:
        """Analyze warning messages"""
        warning_entities = [e for e in self.entities if e.get('entity_type') == 'WARNING']
        
        warnings = []
        warning_types = Counter()
        
        for entity in warning_entities:
            warning = {
                'timestamp': entity.get('timestamp'),
                'level': entity.get('level'),
                'pattern_name': entity.get('pattern_name'),
                'raw_line': entity.get('raw_line', ''),
                'entity_id': entity.get('entity_id')
            }
            warnings.append(warning)
            warning_types[entity.get('pattern_name', 'unknown')] += 1
        
        warnings.sort(key=safe_timestamp_key)
        
        return {
            'total_warnings': len(warnings),
            'warning_details': warnings,
            'warning_types': dict(warning_types),
            'recent_warnings': warnings[-5:] if warnings else []
        }
    
    def _analyze_connectivity(self) -> Dict[str, Any]:
        """Analyze network connectivity and communication issues"""
        # Look for connection-related patterns and errors
        connectivity_issues = []
        
        for entity in self.entities:
            # Check for connection-related errors
            raw_line = entity.get('raw_line', '').lower()
            if any(keyword in raw_line for keyword in ['connection', 'network', 'timeout', 'unreachable', 'refused']):
                connectivity_issues.append({
                    'timestamp': entity.get('timestamp'),
                    'entity_type': entity.get('entity_type'),
                    'issue_type': 'connection',
                    'description': entity.get('raw_line', ''),
                    'pattern_name': entity.get('pattern_name')
                })
            
            # Check for SST communication failures
            if entity.get('entity_type') == 'STATE_TRANSFER':
                error_msg = entity.get('current_error_message', '').lower()
                if any(keyword in error_msg for keyword in ['communication', 'network', 'connection', 'timeout']):
                    connectivity_issues.append({
                        'timestamp': entity.get('start_timestamp') or entity.get('timestamp'),
                        'entity_type': 'SST_COMMUNICATION',
                        'issue_type': 'sst_communication',
                        'description': entity.get('current_error_message', ''),
                        'donor': entity.get('donor_node'),
                        'joiner': entity.get('joiner_node')
                    })
        
        connectivity_issues.sort(key=safe_timestamp_key)
        
        return {
            'total_issues': len(connectivity_issues),
            'issue_details': connectivity_issues,
            'has_connectivity_problems': len(connectivity_issues) > 0,
            'issue_types': Counter(issue['issue_type'] for issue in connectivity_issues)
        }
    
    # Helper methods
    def _get_log_timespan(self) -> Optional[Dict[str, str]]:
        """Calculate the timespan covered by the log"""
        timestamps = []
        
        for entity in self.entities:
            if entity.get('start_timestamp'):
                timestamps.append(entity['start_timestamp'])
            if entity.get('end_timestamp'):
                timestamps.append(entity['end_timestamp'])
            if entity.get('timestamp'):
                timestamps.append(entity['timestamp'])
        
        if not timestamps:
            return None
            
        timestamps.sort()
        return {
            'start': timestamps[0],
            'end': timestamps[-1],
            'duration': str(datetime.fromisoformat(timestamps[-1].replace('Z', '+00:00')) - 
                          datetime.fromisoformat(timestamps[0].replace('Z', '+00:00')))
        }
    
    def _group_ssts_by_status(self, sst_entities: List[Dict]) -> Dict[str, int]:
        """Group SST sessions by their final status"""
        status_counts = Counter()
        for entity in sst_entities:
            status = entity.get('current_transfer_status', 'unknown')
            status_counts[status] += 1
        return dict(status_counts)
    
    def _group_ssts_by_method(self, sst_entities: List[Dict]) -> Dict[str, int]:
        """Group SST sessions by transfer method"""
        method_counts = Counter()
        for entity in sst_entities:
            method = entity.get('transfer_method', 'unknown')
            method_counts[method] += 1
        return dict(method_counts)
    
    def _calculate_duration_stats(self, sst_entities: List[Dict]) -> Dict[str, Any]:
        """Calculate duration statistics for SST sessions"""
        durations = []
        for entity in sst_entities:
            duration = entity.get('duration_seconds')
            if duration and duration > 0:
                durations.append(duration)
        
        if not durations:
            return {'count': 0, 'message': 'No duration data available'}
        
        durations.sort()
        return {
            'count': len(durations),
            'min_seconds': min(durations),
            'max_seconds': max(durations),
            'avg_seconds': sum(durations) / len(durations),
            'median_seconds': durations[len(durations) // 2],
            'total_seconds': sum(durations)
        }
    
    def _get_session_details(self, sst_entities: List[Dict]) -> List[Dict[str, Any]]:
        """Get detailed information for each SST session"""
        sessions = []
        for entity in sst_entities:
            session = {
                'id': entity.get('entity_id'),
                'method': entity.get('transfer_method', 'unknown'),
                'status': entity.get('current_transfer_status', 'unknown'),
                'start_time': entity.get('start_timestamp'),
                'end_time': entity.get('end_timestamp'),
                'duration_seconds': entity.get('duration_seconds'),
                'donor': entity.get('donor_node', 'unknown'),
                'joiner': entity.get('joiner_node', 'unknown'),
                'donor_address': entity.get('donor_address', ''),
                'joiner_address': entity.get('joiner_address', ''),
                'error_message': entity.get('current_error_message', ''),
                'bytes_transferred': entity.get('current_transferred_bytes', 0),
                'progress_percentage': entity.get('current_progress_percentage', 0),
                'seqno_start': entity.get('current_seqno_start'),
                'seqno_end': entity.get('current_seqno_end'),
                'pattern_matches': entity.get('pattern_name', ''),
                'lifecycle_phase': entity.get('lifecycle_phase', ''),
                'raw_lines': entity.get('raw_line', '')
            }
            sessions.append(session)
        
        # Sort by start time
        sessions.sort(key=safe_timestamp_key)
        return sessions
    
    def _get_entity_description(self, entity: Dict[str, Any]) -> str:
        """Generate a human-readable description for an entity"""
        entity_type = entity.get('entity_type', 'unknown')
        
        if entity_type == 'STATE_TRANSFER':
            method = entity.get('transfer_method', 'unknown')
            status = entity.get('current_transfer_status', 'unknown')
            return f"SST session ({method}) - {status}"
        
        return f"{entity_type} event"
    
    def _identify_node_roles(self) -> Dict[str, Any]:
        """Identify node roles from the entities"""
        donors = set()
        joiners = set()
        
        for entity in self.entities:
            if entity.get('entity_type') == 'STATE_TRANSFER':
                donor = entity.get('donor_node')
                joiner = entity.get('joiner_node')
                if donor and donor != 'unknown_donor':
                    donors.add(donor)
                if joiner and joiner != 'unknown_joiner':
                    joiners.add(joiner)
        
        return {
            'donors': list(donors),
            'joiners': list(joiners),
            'donor_count': len(donors),
            'joiner_count': len(joiners)
        }
    
    def _analyze_cluster_membership(self) -> Dict[str, Any]:
        """Analyze cluster membership changes"""
        # This would be expanded when we have cluster view entities
        return {'status': 'Not implemented - requires cluster view entities'}
    
    def _track_state_changes(self) -> List[Dict[str, Any]]:
        """Track node state changes"""
        # This would be expanded when we have node state entities
        return []
    
    def _calculate_success_rate(self, sst_entities: List[Dict]) -> Dict[str, Any]:
        """Calculate SST success rate"""
        if not sst_entities:
            return {'rate': 0, 'total': 0, 'successful': 0, 'failed': 0}
        
        successful = 0
        failed = 0
        
        for entity in sst_entities:
            status = entity.get('current_transfer_status', '').lower()
            if 'complete' in status or 'success' in status:
                successful += 1
            elif 'fail' in status or 'error' in status:
                failed += 1
        
        total = len(sst_entities)
        rate = (successful / total * 100) if total > 0 else 0
        
        return {
            'rate': round(rate, 1),
            'total': total,
            'successful': successful,
            'failed': failed,
            'other': total - successful - failed
        }
    
    def _calculate_average_duration(self, sst_entities: List[Dict]) -> Optional[float]:
        """Calculate average SST duration"""
        durations = [e.get('duration_seconds', 0) for e in sst_entities if e.get('duration_seconds', 0) > 0]
        return sum(durations) / len(durations) if durations else None
    
    def _analyze_throughput(self, sst_entities: List[Dict]) -> Dict[str, Any]:
        """Analyze throughput statistics"""
        # This would be expanded when we have transfer rate data
        return {'status': 'Not implemented - requires transfer rate data'}


def print_analysis(analysis: Dict[str, Any]):
    """Print the analysis in a structured, readable format"""
    print("="*80)
    print("GALERA LOG ANALYSIS SUMMARY")
    print("="*80)
    
    # Overview
    overview = analysis['overview']
    print(f"\n📊 OVERVIEW")
    print(f"   Analysis Time: {overview.get('analysis_time', 'Unknown')}")
    print(f"   Total Entities: {overview.get('total_entities', 0)}")
    
    if overview.get('log_timespan'):
        timespan = overview['log_timespan']
        print(f"   Log Timespan: {timespan.get('start', 'Unknown')} to {timespan.get('end', 'Unknown')}")
        print(f"   Duration: {timespan.get('duration', 'Unknown')}")
    
    if overview.get('entity_types'):
        print(f"   Entity Types:")
        for entity_type, count in overview['entity_types'].items():
            print(f"     {entity_type}: {count}")
    
    # Node Analysis
    nodes = analysis['nodes']
    print(f"\n🖥️  CLUSTER NODES")
    print(f"   Total Nodes Detected: {nodes.get('total_nodes', 0)}")
    print(f"   Nodes with Names: {nodes.get('nodes_with_names', 0)}")
    print(f"   Nodes with IDs: {nodes.get('nodes_with_ids', 0)}")
    
    if nodes.get('unique_node_names'):
        print(f"   Node Names: {', '.join(nodes['unique_node_names'])}")
    
    if nodes.get('node_details'):
        print(f"   Node Details:")
        for node_key, node_info in nodes['node_details'].items():
            name = node_info.get('node_name', 'Unknown')
            node_id = node_info.get('node_id', 'Unknown')
            states = len(node_info.get('states', []))
            print(f"     {name} ({node_id}): {states} state changes")
            
            # Show recent states
            recent_states = node_info.get('states', [])[-3:] if node_info.get('states') else []
            for state in recent_states:
                print(f"       {state.get('timestamp', 'Unknown')}: {state.get('state', 'Unknown')}")
    
    # Cluster Views
    views = analysis['cluster_views']
    print(f"\n🔗 CLUSTER VIEWS")
    if views.get('total_views', 0) == 0:
        print(f"   {views.get('message', 'No cluster view changes found')}")
    else:
        print(f"   Total View Changes: {views['total_views']}")
        print(f"   Cluster Size: {views.get('min_cluster_size', 0)} → {views.get('final_cluster_size', 0)} (max: {views.get('max_cluster_size', 0)})")
        
        membership = views.get('membership_changes', {})
        if membership.get('total_joins', 0) > 0 or membership.get('total_leaves', 0) > 0:
            print(f"   Membership Changes: +{membership.get('total_joins', 0)} joined, -{membership.get('total_leaves', 0)} left")
        
        # Show recent view changes
        recent_views = views.get('view_changes', [])[-3:]
        for view in recent_views:
            print(f"     {view.get('timestamp', 'Unknown')}: {view.get('member_count', 0)} members, state: {view.get('cluster_state', 'Unknown')}")
    
    # SST Sessions
    sst = analysis['sst_sessions']
    print(f"\n🔄 SST SESSIONS")
    if sst.get('total_sessions', 0) == 0:
        print(f"   {sst.get('summary', 'No SST sessions found')}")
    else:
        print(f"   Total Sessions: {sst['total_sessions']}")
        
        if sst.get('by_status'):
            print(f"   By Status:")
            for status, count in sst['by_status'].items():
                print(f"     {status}: {count}")
        
        if sst.get('by_method'):
            print(f"   By Method:")
            for method, count in sst['by_method'].items():
                print(f"     {method}: {count}")
        
        if sst.get('duration_stats') and sst['duration_stats'].get('count', 0) > 0:
            stats = sst['duration_stats']
            print(f"   Duration Statistics:")
            print(f"     Count: {stats['count']}")
            print(f"     Average: {format_duration(stats['avg_seconds'])}")
            print(f"     Min: {format_duration(stats['min_seconds'])}")
            print(f"     Max: {format_duration(stats['max_seconds'])}")
            print(f"     Total: {format_duration(stats['total_seconds'])}")
        
        # Show detailed session information
        sessions = sst.get('session_details', [])
        if sessions:
            print(f"   Session Details:")
            for session in sessions[-5:]:  # Show last 5 sessions
                donor = session.get('donor', 'unknown')
                joiner = session.get('joiner', 'unknown') 
                method = session.get('method', 'unknown')
                status = session.get('status', 'unknown')
                duration = session.get('duration_seconds')
                error = session.get('error_message', '')
                
                duration_str = f" ({format_duration(duration)})" if duration and duration > 0 else ""
                error_str = f" - {error}" if error else ""
                
                print(f"     {session.get('start_time', 'Unknown')}: {donor} → {joiner} ({method}) {status}{duration_str}{error_str}")
    
    # Performance
    performance = analysis['performance']
    print(f"\n📈 PERFORMANCE")
    success_rate = performance.get('sst_success_rate', {})
    if success_rate.get('total', 0) > 0:
        print(f"   SST Success Rate: {success_rate['rate']}% ({success_rate['successful']}/{success_rate['total']})")
        if success_rate.get('failed', 0) > 0:
            print(f"   Failed Sessions: {success_rate['failed']}")
    
    avg_duration = performance.get('average_duration')
    if avg_duration:
        print(f"   Average SST Duration: {format_duration(avg_duration)}")
    
    # Connectivity Issues
    connectivity = analysis['connectivity']
    if connectivity.get('has_connectivity_problems', False):
        print(f"\n🌐 CONNECTIVITY")
        print(f"   Total Issues: {connectivity['total_issues']}")
        
        issue_types = connectivity.get('issue_types', {})
        for issue_type, count in issue_types.items():
            print(f"   {issue_type}: {count}")
        
        # Show recent connectivity issues
        recent_issues = connectivity.get('issue_details', [])[-3:]
        for issue in recent_issues:
            print(f"     {issue.get('timestamp', 'Unknown')}: {issue.get('issue_type', 'unknown')} - {issue.get('description', 'No description')[:100]}...")
    
    # Warnings
    warnings = analysis['warnings']
    if warnings.get('total_warnings', 0) > 0:
        print(f"\n⚠️  WARNINGS")
        print(f"   Total Warnings: {warnings['total_warnings']}")
        
        warning_types = warnings.get('warning_types', {})
        for warning_type, count in warning_types.items():
            print(f"   {warning_type}: {count}")
        
        # Show recent warnings
        recent_warnings = warnings.get('recent_warnings', [])
        for warning in recent_warnings:
            print(f"     {warning.get('timestamp', 'Unknown')}: {warning.get('raw_line', 'No details')[:100]}...")
    
    # Errors
    errors = analysis['errors']
    if errors.get('error_count', 0) > 0:
        print(f"\n❌ ERRORS")
        print(f"   Total Errors: {errors['error_count']}")
        
        for error in errors['errors'][:5]:  # Show first 5
            print(f"     {error.get('entity_id', 'unknown')}: {error.get('error_message', error.get('status', 'Unknown error'))}")
    
    # Recent Timeline (last 15 events)
    timeline = analysis['timeline']
    if timeline:
        print(f"\n🕒 RECENT TIMELINE (last 15 events)")
        for event in timeline[-15:]:
            timestamp = event.get('timestamp', 'Unknown')
            if len(timestamp) > 19:  # Trim microseconds for readability
                timestamp = timestamp[:19]
            entity_type = event.get('entity_type', 'unknown')
            description = event.get('description', 'Unknown event')
            print(f"   {timestamp} [{entity_type}]: {description}")
    
    print("\n" + "="*80)


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 0.001:
        return f"{seconds*1000:.1f}ms"
    elif seconds < 1.0:
        return f"{seconds*1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def print_sst_sessions_timeline(analyzer: GraAnalyzer):
    """Print SST sessions timeline in tree format similar to original 'gra' tool"""
    print("\n" + "="*80)
    print("SST SESSIONS TIMELINE")
    print("="*80)
    
    # Get SST sessions using chronological tracker
    tracker = SSTSessionTracker(analyzer.log_file_path)
    sessions = tracker.process_entities(analyzer.entities)
    
    if not sessions:
        print("No SST sessions detected in the log.")
        return
    
    def format_timestamp(ts):
        """Format timestamp for display"""
        if not ts:
            return "Unknown"
        # Trim microseconds for better readability
        if len(ts) > 19 and 'T' in ts:
            return ts[:19].replace('T', ' ')
        return ts
        
    print(f"\nTotal SST Sessions: {len(sessions)}")
    print("-" * 40)
    
    for i, session in enumerate(sessions, 1):
        print(f"\n[{i}] SST Session")
        
        start_time_str = format_timestamp(session.request_time)
        end_time_str = format_timestamp(session.completion_time)
        
        # Timeline display
        if session.request_time:
            print(f"    ├─ Start:  {start_time_str}")
            print(f"    ├─ Status: {session.status}")
            
            # Show donor and joiner info
            if session.donor:
                print(f"    ├─ Donor:  {session.donor}")
            
            if session.joiner:
                print(f"    ├─ Joiner: {session.joiner}")
                
            if session.method:
                print(f"    ├─ Method: {session.method}")
                
            # Show duration if available
            if session.duration_seconds and session.duration_seconds > 0:
                duration_str = format_duration(session.duration_seconds)
                print(f"    ├─ Duration: {duration_str}")
            
        if session.completion_time and session.status in ['COMPLETED', 'FAILED']:
            print(f"    └─ End:    {end_time_str}")
            
            if session.error_message:
                # Truncate very long error messages
                error_msg = session.error_message
                if len(error_msg) > 120:
                    error_msg = error_msg[:117] + "..."
                print(f"       Error:  {error_msg}")
        elif session.status == "ONGOING":
            print(f"    └─ Status: Session still ongoing (incomplete)")
        elif session.status == "INCOMPLETE":
            print(f"    └─ Status: Session incomplete (no completion found)")
            
        # Show session events count
        if hasattr(session, 'events') and session.events:
            print(f"       Events: {len(session.events)} log entries")
    
    print("\n" + "="*80)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Analyze Galera logs using grap.py output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('logfile', nargs='?', help='Galera log file to analyze')
    parser.add_argument('--stdin', action='store_true', help='Read JSON from stdin (from grap.py)')
    parser.add_argument('--sst-sessions', action='store_true', help='Show detailed SST session timeline (like gra format)')
    parser.add_argument('--version', action='version', version=f'gra-analyzer {__version__}')
    
    args = parser.parse_args()
    
    if not args.logfile and not args.stdin:
        parser.error("Either provide a logfile or use --stdin")
    
    if args.logfile and args.stdin:
        parser.error("Cannot use both logfile and --stdin")
    
    analyzer = GraAnalyzer()
    
    if args.stdin:
        # Read JSON from stdin
        json_input = sys.stdin.read()
        if not analyzer.load_from_json(json_input):
            sys.exit(1)
    else:
        # Run grap.py on the logfile
        logfile = Path(args.logfile)
        if not logfile.exists():
            print(f"Error: Log file '{args.logfile}' does not exist", file=sys.stderr)
            sys.exit(1)
        
        if not analyzer.load_from_grap(logfile):
            sys.exit(1)
    
    # Perform analysis
    analysis = analyzer.analyze()
    
    # Print results
    if args.sst_sessions:
        print_sst_sessions_timeline(analyzer)
    else:
        print_analysis(analysis)


if __name__ == '__main__':
    main()