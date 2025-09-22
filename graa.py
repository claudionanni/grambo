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

__version__ = "1.0.0-alpha1"
__author__ = "Claudio Nanni"
__description__ = "Galera Log Analysis Summary Tool - processes grap.py output"


class GraAnalyzer:
    """Main analyzer class that processes grap output and creates structured summaries"""
    
    def __init__(self):
        self.entities = []
        self.metadata = {}
        self.analysis_timestamp = datetime.now()
        
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
            'sst_sessions': self._analyze_sst_sessions(),
            'timeline': self._analyze_timeline(),
            'errors': self._analyze_errors(),
            'cluster_state': self._analyze_cluster_state(),
            'performance': self._analyze_performance()
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
        
        # Sort by timestamp
        events.sort(key=lambda x: x['timestamp'])
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
                'error_message': entity.get('current_error_message', ''),
                'bytes_transferred': entity.get('current_transferred_bytes', 0)
            }
            sessions.append(session)
        
        # Sort by start time
        sessions.sort(key=lambda x: x.get('start_time', ''))
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


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


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
    
    # Errors
    errors = analysis['errors']
    if errors.get('error_count', 0) > 0 or errors.get('warning_count', 0) > 0:
        print(f"\n⚠️  ISSUES")
        if errors.get('error_count', 0) > 0:
            print(f"   Errors: {errors['error_count']}")
            for error in errors['errors'][:5]:  # Show first 5
                print(f"     {error.get('entity_id', 'unknown')}: {error.get('error_message', error.get('status', 'Unknown error'))}")
        
        if errors.get('warning_count', 0) > 0:
            print(f"   Warnings: {errors['warning_count']}")
            for warning in errors['warnings'][:5]:  # Show first 5
                print(f"     {warning.get('entity_id', 'unknown')}: {warning.get('message', 'Unknown warning')}")
    
    # Recent Timeline (last 10 events)
    timeline = analysis['timeline']
    if timeline:
        print(f"\n🕒 RECENT TIMELINE (last 10 events)")
        for event in timeline[-10:]:
            timestamp = event.get('timestamp', 'Unknown')
            if len(timestamp) > 19:  # Trim microseconds for readability
                timestamp = timestamp[:19]
            print(f"   {timestamp}: {event.get('description', 'Unknown event')}")
    
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
    print_analysis(analysis)


if __name__ == '__main__':
    main()