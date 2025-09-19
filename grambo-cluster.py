#!/usr/bin/env python3
"""
Grambo Cluster Analyzer
=======================

Multi-node Galera cluster log correlation tool.
Takes JSON outputs from gramboo.py and provides cluster-wide analysis.

Usage:
    python3 grambo-cluster.py node1.json node2.json node3.json
    python3 grambo-cluster.py --node-names db1,db2,db3 file1.json file2.json file3.json
    python3 grambo-cluster.py --node db1:db1.json --node db2:db2.json
    python3 grambo-cluster.py --format=json cluster-analysis/*.json
"""

import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set, Tuple, NamedTuple
from dataclasses import dataclass, field
from collections import defaultdict
import re
import os

class NodeData(NamedTuple):
    """Container for node analysis data"""
    name: str
    file_path: str
    data: Dict[str, Any]

@dataclass
class ClusterEvent:
    """Unified cluster event across all nodes"""
    timestamp: datetime
    node: str
    event_type: str
    details: Dict[str, Any]
    original_event: Dict[str, Any] = field(default_factory=dict)

@dataclass 
class SSTWorkflow:
    """Complete SST workflow across joiner and donor"""
    request_time: datetime
    joiner_node: str
    donor_node: Optional[str] = None
    method: Optional[str] = None
    completion_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    status: str = "requested"  # requested, started, completed, failed
    events: List[ClusterEvent] = field(default_factory=list)

@dataclass
class ClusterView:
    """Cluster membership view at a specific time"""
    timestamp: datetime
    node: str
    members: Set[str]
    seqno: Optional[int] = None
    view_id: Optional[str] = None

@dataclass
class StateTransition:
    """Node state transition event"""
    timestamp: datetime
    node: str
    from_state: str
    to_state: str
    seqno: Optional[int] = None

class GramboClusterAnalyzer:
    """Analyzes multiple gramboo JSON outputs for cluster-wide insights"""
    
    def __init__(self, output_format: str = 'text', quiet: bool = False):
        self.nodes: List[NodeData] = []
        self.output_format = output_format
        self.quiet = quiet
        self.cluster_events: List[ClusterEvent] = []
        self.sst_workflows: List[SSTWorkflow] = []
        self.cluster_views: List[ClusterView] = []
        self.state_transitions: List[StateTransition] = []
        
    def log(self, message: str) -> None:
        """Log message unless in quiet mode"""
        if not self.quiet:
            print(message)
    
    def load_node_data(self, file_path: str, node_name: Optional[str] = None) -> None:
        """Load gramboo JSON output for a node"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if not node_name:
                # Extract actual Galera node name from the JSON data
                cluster_info = data.get('cluster_info', {})
                galera_node_name = cluster_info.get('local_node_name')
                if galera_node_name:
                    node_name = galera_node_name
                else:
                    # Fallback to filename if no local_node_name found
                    base_name = os.path.basename(file_path)
                    node_name = base_name.split('.')[0]  # Remove .json extension
                    self.log(f"⚠ No local_node_name found, using filename: {node_name}")
            
            # Ensure node_name is not None
            if not node_name:
                node_name = "unknown_node"
                
            self.nodes.append(NodeData(node_name, file_path, data))
            self.log(f"✓ Loaded {node_name} from {file_path}")
            
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"✗ Error loading {file_path}: {e}", file=sys.stderr)
            sys.exit(1)
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse timestamp from various formats"""
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%m-%d %H:%M:%S',
            '%H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def extract_cluster_events(self) -> None:
        """Extract and normalize events from all nodes"""
        for node in self.nodes:
            node_data = node.data
            
            # Extract from detailed_events if available (newer format)
            detailed_events = node_data.get('detailed_events', [])
            for event in detailed_events:
                timestamp_str = event.get('timestamp')
                if not timestamp_str:
                    continue
                    
                timestamp = self.parse_timestamp(timestamp_str)
                if not timestamp:
                    continue
                
                event_type = event.get('event_type')
                
                # State Transition Events
                if event_type == 'state_transition' and event.get('state_transition'):
                    st = event['state_transition']
                    cluster_event = ClusterEvent(
                        timestamp=timestamp,
                        node=node.name,
                        event_type='state_transition',
                        details={
                            'from_state': st.get('from_state'),
                            'to_state': st.get('to_state'),
                            'sequence_number': st.get('sequence_number')
                        },
                        original_event=event
                    )
                    self.cluster_events.append(cluster_event)
                
                # Cluster View Events
                elif event_type == 'cluster_view' and event.get('cluster_view'):
                    cv = event['cluster_view']
                    members = []
                    if cv.get('members'):
                        for uuid, member in cv['members'].items():
                            if member.get('name'):
                                members.append(member['name'])
                    
                    cluster_event = ClusterEvent(
                        timestamp=timestamp,
                        node=node.name,
                        event_type='cluster_view',
                        details={
                            'view_id': cv.get('view_id'),
                            'status': cv.get('status'),
                            'members': ','.join(sorted(members)),
                            'member_count': len(members)
                        },
                        original_event=event
                    )
                    self.cluster_events.append(cluster_event)
                
                # SST Events (from metadata or raw message)
                elif event_type == 'sst_event' or 'SST' in event.get('raw_message', ''):
                    cluster_event = ClusterEvent(
                        timestamp=timestamp,
                        node=node.name,
                        event_type='sst_event',
                        details={
                            'message': event.get('raw_message', ''),
                            'metadata': event.get('metadata', {})
                        },
                        original_event=event
                    )
                    self.cluster_events.append(cluster_event)
            
            # Fallback: Extract from st_workflows (legacy format)
            st_workflows = node_data.get('st_workflows', [])
            for workflow in st_workflows:
                # SST Request Event
                if 'requested_at' in workflow:
                    timestamp = self.parse_timestamp(workflow['requested_at'])
                    if timestamp:
                        event = ClusterEvent(
                            timestamp=timestamp,
                            node=node.name,
                            event_type='sst_request',
                            details={
                                'joiner': workflow.get('joiner'),
                                'donor': workflow.get('donor'),
                                'decision': workflow.get('decision')
                            },
                            original_event=workflow
                        )
                        self.cluster_events.append(event)
                
                # SST Status Events
                if 'sst' in workflow and workflow['sst']:
                    sst = workflow['sst']
                    if 'timestamp' in sst:
                        timestamp = self.parse_timestamp(sst['timestamp'])
                        if timestamp:
                            event = ClusterEvent(
                                timestamp=timestamp,
                                node=node.name,
                                event_type='sst_event',
                                details={
                                    'status': sst.get('status'),
                                    'joiner': workflow.get('joiner'),
                                    'donor': workflow.get('donor')
                                },
                                original_event=sst
                            )
                            self.cluster_events.append(event)
            
            # Extract cluster membership from current_view if available
            cluster_info = node_data.get('cluster_info', {})
            if 'current_view' in cluster_info:
                current_view = cluster_info['current_view']
                if 'timestamp' in current_view:
                    timestamp = self.parse_timestamp(current_view['timestamp'])
                    if timestamp:
                        members = set()
                        view_members = current_view.get('members', {})
                        for uuid, member_info in view_members.items():
                            if member_info.get('name'):
                                members.add(member_info['name'])
                        
                        event = ClusterEvent(
                            timestamp=timestamp,
                            node=node.name,
                            event_type='cluster_view',
                            details={
                                'view_id': current_view.get('view_id'),
                                'status': current_view.get('status'),
                                'members': ','.join(sorted(members)),
                                'member_count': len(members)
                            },
                            original_event=current_view
                        )
                        self.cluster_events.append(event)
        
        # Sort all events by timestamp
        self.cluster_events.sort(key=lambda e: e.timestamp)
        self.log(f"✓ Extracted {len(self.cluster_events)} cluster events")
    
    def correlate_sst_workflows(self) -> None:
        """Find and correlate SST request/donor/completion sequences"""
        workflows = {}
        
        for event in self.cluster_events:
            if event.event_type in ['sst_request', 'sst_event']:
                details = event.details
                
                # Create workflow key based on joiner and approximate time
                joiner = details.get('joiner')
                if joiner:
                    # Group workflows within 5-minute windows
                    time_key = event.timestamp.replace(minute=event.timestamp.minute//5*5, second=0, microsecond=0)
                    workflow_key = f"{joiner}_{time_key.isoformat()}"
                    
                    if workflow_key not in workflows:
                        workflows[workflow_key] = SSTWorkflow(
                            request_time=event.timestamp,
                            joiner_node=joiner,
                            donor_node=details.get('donor'),
                            method=details.get('method', 'unknown')
                        )
                    
                    workflow = workflows[workflow_key]
                    workflow.events.append(event)
                    
                    # Update workflow based on event
                    if event.event_type == 'sst_request':
                        workflow.status = 'requested'
                        if not workflow.donor_node:
                            workflow.donor_node = details.get('donor')
                    
                    elif event.event_type == 'sst_event':
                        status = details.get('status', '').lower()
                        decision = details.get('decision', '').lower()
                        
                        if 'failed' in status or 'failed' in decision:
                            workflow.status = 'failed'
                        elif 'success' in status or 'complete' in status:
                            workflow.status = 'completed'
                            workflow.completion_time = event.timestamp
                            if workflow.request_time:
                                workflow.duration_seconds = (event.timestamp - workflow.request_time).total_seconds()
                        elif 'start' in status:
                            workflow.status = 'started'
        
        self.sst_workflows = list(workflows.values())
        self.log(f"✓ Identified {len(self.sst_workflows)} SST workflows")
    
    def analyze_cluster_views(self) -> None:
        """Extract cluster view changes and detect inconsistencies"""
        for event in self.cluster_events:
            if event.event_type == 'cluster_view':
                details = event.details
                members_str = details.get('members', '')
                
                # Parse member list (assuming format like "node1,node2,node3")
                members = set()
                if members_str:
                    members = set(m.strip() for m in members_str.split(',') if m.strip())
                
                view = ClusterView(
                    timestamp=event.timestamp,
                    node=event.node,
                    members=members,
                    seqno=details.get('seqno'),
                    view_id=details.get('view_id')
                )
                self.cluster_views.append(view)
        
        self.log(f"✓ Analyzed {len(self.cluster_views)} cluster view changes")
    
    def extract_state_transitions(self) -> None:
        """Extract state transition events"""
        for event in self.cluster_events:
            if event.event_type == 'state_transition':
                details = event.details
                
                transition = StateTransition(
                    timestamp=event.timestamp,
                    node=event.node,
                    from_state=details.get('from_state', 'unknown'),
                    to_state=details.get('to_state', 'unknown'),
                    seqno=details.get('sequence_number')
                )
                self.state_transitions.append(transition)
        
        self.log(f"✓ Extracted {len(self.state_transitions)} state transitions")
    
    def analyze_cluster(self) -> None:
        """Perform complete cluster analysis"""
        self.log("🔍 Starting cluster analysis...")
        
        self.extract_cluster_events()
        self.correlate_sst_workflows()
        self.analyze_cluster_views()
        self.extract_state_transitions()
        
        self.log("✅ Cluster analysis complete")
    
    def detect_split_brain(self) -> List[Dict[str, Any]]:
        """Detect split-brain scenarios from cluster views"""
        split_brain_events = []
        
        # Group views by timestamp (within a small window)
        time_groups = defaultdict(list)
        for view in self.cluster_views:
            # Round to nearest minute for grouping
            rounded_time = view.timestamp.replace(second=0, microsecond=0)
            time_groups[rounded_time].append(view)
        
        for timestamp, views in time_groups.items():
            if len(views) > 1:
                # Check if nodes have different views of cluster membership
                unique_memberships = set()
                for view in views:
                    membership_str = ','.join(sorted(view.members))
                    unique_memberships.add(membership_str)
                
                if len(unique_memberships) > 1:
                    split_brain_events.append({
                        'timestamp': timestamp,
                        'views': [{'node': v.node, 'members': v.members} for v in views]
                    })
        
        return split_brain_events
    
    def format_output_text(self) -> str:
        """Generate human-readable text output"""
        output = []
        
        # Header
        output.append("=" * 80)
        output.append("| G R A M B O - GALERA CLUSTER MULTI NODE LOG ANALYZER")
        output.append("=" * 80)
        
        # Cluster Overview
        output.append("\n🌐 CLUSTER OVERVIEW")
        output.append("-" * 50)
        node_names = [node.name for node in self.nodes]
        output.append(f"  Nodes: {', '.join(node_names)}")
        
        if self.cluster_events:
            start_time = min(e.timestamp for e in self.cluster_events)
            end_time = max(e.timestamp for e in self.cluster_events)
            output.append(f"  Time Range: {start_time} - {end_time}")
            output.append(f"  Total Events: {len(self.cluster_events)}")
        
        # SST/IST Workflows
        if self.sst_workflows:
            output.append("\n🔄 SST/IST WORKFLOWS")
            output.append("-" * 50)
            
            for workflow in self.sst_workflows[:10]:  # Limit to first 10
                duration_str = ""
                if workflow.duration_seconds:
                    duration_str = f" ({workflow.duration_seconds:.0f}s)"
                
                donor_info = f" → {workflow.donor_node}" if workflow.donor_node else ""
                method_info = f" ({workflow.method})" if workflow.method else ""
                
                output.append(f"  {workflow.request_time} | {workflow.joiner_node}{donor_info} | "
                            f"{workflow.status.upper()}{method_info}{duration_str}")
        
        # Split-Brain Detection
        split_brain_events = self.detect_split_brain()
        if split_brain_events:
            output.append("\n⚠️  SPLIT-BRAIN SCENARIOS")
            output.append("-" * 50)
            
            for event in split_brain_events[:5]:  # Limit to first 5
                output.append(f"  {event['timestamp']} | Different cluster views:")
                for view in event['views']:
                    members_str = ', '.join(sorted(view['members']))
                    output.append(f"    └─ {view['node']}: {{{members_str}}}")
        
        # State Transitions Timeline
        if self.state_transitions:
            output.append("\n🔄 STATE TRANSITIONS")
            output.append("-" * 50)
            
            for transition in self.state_transitions[:15]:  # Limit to first 15
                seqno_info = f" (seqno: {transition.seqno})" if transition.seqno else ""
                output.append(f"  {transition.timestamp} | {transition.node} | "
                            f"{transition.from_state} → {transition.to_state}{seqno_info}")
        
        return '\n'.join(output)
    
    def format_output_json(self) -> str:
        """Generate JSON output"""
        result = {
            'cluster_analysis': {
                'nodes': [node.name for node in self.nodes],
                'summary': {
                    'total_events': len(self.cluster_events),
                    'sst_workflows': len(self.sst_workflows),
                    'state_transitions': len(self.state_transitions),
                    'cluster_views': len(self.cluster_views)
                },
                'sst_workflows': [
                    {
                        'request_time': workflow.request_time.isoformat(),
                        'joiner': workflow.joiner_node,
                        'donor': workflow.donor_node,
                        'method': workflow.method,
                        'status': workflow.status,
                        'duration_seconds': workflow.duration_seconds,
                        'completion_time': workflow.completion_time.isoformat() if workflow.completion_time else None
                    }
                    for workflow in self.sst_workflows
                ],
                'split_brain_events': [
                    {
                        'timestamp': event['timestamp'].isoformat() if isinstance(event['timestamp'], datetime) else str(event['timestamp']),
                        'views': [
                            {
                                'node': view['node'],
                                'members': list(view['members']) if isinstance(view['members'], set) else view['members']
                            }
                            for view in event['views']
                        ]
                    }
                    for event in self.detect_split_brain()
                ],
                'state_transitions': [
                    {
                        'timestamp': t.timestamp.isoformat(),
                        'node': t.node,
                        'from_state': t.from_state,
                        'to_state': t.to_state,
                        'seqno': t.seqno
                    }
                    for t in self.state_transitions
                ]
            }
        }
        
        return json.dumps(result, indent=2)
    
    def generate_output(self) -> str:
        """Generate output in the specified format"""
        if self.output_format == 'json':
            return self.format_output_json()
        else:
            return self.format_output_text()

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Grambo Cluster Analyzer - Multi-node Galera log correlation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s node1.json node2.json node3.json
  %(prog)s --node-names db1,db2,db3 file1.json file2.json file3.json
  %(prog)s --node db1:db1.json --node db2:db2.json
  %(prog)s --format=json cluster-logs/*.json
        """
    )
    
    parser.add_argument('files', nargs='*', 
                       help='JSON files from gramboo.py output')
    
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    
    parser.add_argument('--node-names', 
                       help='Comma-separated node names corresponding to input files')
    
    parser.add_argument('--node', action='append', dest='node_mappings',
                       help='Node mapping in format name:file.json (can be used multiple times)')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_arguments()
    
    if not args.files and not args.node_mappings:
        print("Error: No input files specified", file=sys.stderr)
        sys.exit(1)
    
    analyzer = GramboClusterAnalyzer(output_format=args.format, quiet=(args.format == 'json'))
    
    # Handle explicit node mappings
    if args.node_mappings:
        for mapping in args.node_mappings:
            if ':' not in mapping:
                print(f"Error: Invalid node mapping format: {mapping}", file=sys.stderr)
                print("Expected format: nodename:filename.json", file=sys.stderr)
                sys.exit(1)
            
            node_name, file_path = mapping.split(':', 1)
            analyzer.load_node_data(file_path, node_name)
    
    # Handle regular files with optional node names
    if args.files:
        node_names = []
        if args.node_names:
            node_names = [name.strip() for name in args.node_names.split(',')]
        
        for i, file_path in enumerate(args.files):
            node_name = node_names[i] if i < len(node_names) else None
            analyzer.load_node_data(file_path, node_name)
    
    # Perform analysis
    analyzer.analyze_cluster()
    
    # Generate and output results
    output = analyzer.generate_output()
    print("\n" + output)

if __name__ == '__main__':
    main()
