#!/usr/bin/env python3
"""
Process cl407 logs with schema-based extractor

This script processes all log files in the cl407 directory and generates
a comprehensive analysis of the extracted entities.
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.schema_engine import SchemaBasedExtractor


def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def process_all_logs(log_dir: Path):
    """Process all log files in directory"""
    print_banner("Processing CL407 Galera Logs")
    
    # Initialize extractor
    schema_dir = Path(__file__).parent / "schema"
    extractor = SchemaBasedExtractor(schema_dir)
    
    print(f"✓ Loaded {len(extractor.schema_loader.entity_schemas)} entity schemas")
    print(f"✓ Loaded {len(extractor.schema_loader.patterns)} patterns\n")
    
    # Find all log files
    log_files = sorted(log_dir.glob("*.log"))
    
    if not log_files:
        print(f"No log files found in {log_dir}")
        return None
    
    print(f"Found {len(log_files)} log files:")
    for log_file in log_files:
        size_kb = log_file.stat().st_size / 1024
        print(f"  • {log_file.name} ({size_kb:.1f} KB)")
    
    # Process each log file
    print("\n--- Processing Logs ---\n")
    
    all_entities = {
        'core_entities': {},
        'temporal_entities': {}
    }
    
    stats = {
        'total_lines': 0,
        'total_matched': 0,
        'by_file': {}
    }
    
    for log_file in log_files:
        print(f"Processing {log_file.name}...")
        start_time = datetime.now()
        
        try:
            # Process log file
            entities = extractor.process_log_file(log_file)
            
            # Merge entities
            # CORE entities - merge by entity_id
            for entity_type, entity_dict in entities.get('core_entities', {}).items():
                if entity_type not in all_entities['core_entities']:
                    all_entities['core_entities'][entity_type] = {}
                all_entities['core_entities'][entity_type].update(entity_dict)
            
            # TEMPORAL entities - append all
            for entity_type, entity_list in entities.get('temporal_entities', {}).items():
                if entity_type not in all_entities['temporal_entities']:
                    all_entities['temporal_entities'][entity_type] = []
                all_entities['temporal_entities'][entity_type].extend(entity_list)
            
            # Count lines
            with open(log_file, 'r') as f:
                line_count = sum(1 for _ in f)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Calculate matches
            core_count = sum(len(e) for e in entities.get('core_entities', {}).values())
            temporal_count = sum(len(e) for e in entities.get('temporal_entities', {}).values())
            total_count = core_count + temporal_count
            
            stats['total_lines'] += line_count
            stats['total_matched'] += total_count
            stats['by_file'][log_file.name] = {
                'lines': line_count,
                'entities': total_count,
                'duration': duration
            }
            
            print(f"  ✓ {line_count:,} lines, {total_count} entities in {duration:.2f}s")
            
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback
            traceback.print_exc()
    
    return all_entities, stats


def analyze_entities(entities):
    """Analyze extracted entities"""
    print_banner("Entity Analysis")
    
    # Count entities
    core_types = defaultdict(int)
    temporal_types = defaultdict(int)
    
    for entity_type, entity_dict in entities.get('core_entities', {}).items():
        core_types[entity_type] = len(entity_dict)
    
    for entity_type, entity_list in entities.get('temporal_entities', {}).items():
        temporal_types[entity_type] = len(entity_list)
    
    # Display CORE entities
    print("CORE Entities (Immutable):")
    if core_types:
        for entity_type, count in sorted(core_types.items()):
            print(f"  • {entity_type}: {count}")
    else:
        print("  (none)")
    
    # Display TEMPORAL entities
    print("\nTEMPORAL Entities (Time-based):")
    if temporal_types:
        for entity_type, count in sorted(temporal_types.items()):
            print(f"  • {entity_type}: {count}")
    else:
        print("  (none)")
    
    total_core = sum(core_types.values())
    total_temporal = sum(temporal_types.values())
    
    print(f"\nTotal: {total_core} CORE + {total_temporal} TEMPORAL = {total_core + total_temporal} entities")
    
    # Analyze specific entities
    print_banner("Detailed Analysis")
    
    # Clusters
    clusters = entities.get('core_entities', {}).get('Cluster', {})
    if clusters:
        print(f"Clusters Found: {len(clusters)}")
        for cluster_id, cluster in clusters.items():
            print(f"  • {cluster.get('cluster_uuid', 'unknown')}")
            if cluster.get('cluster_name'):
                print(f"    Name: {cluster.get('cluster_name')}")
            print(f"    First seen: {cluster.get('first_seen', 'N/A')}")
    
    # Nodes
    nodes = entities.get('core_entities', {}).get('Node', {})
    if nodes:
        print(f"\nNodes Found: {len(nodes)}")
        for node_id, node in nodes.items():
            uuid = node.get('node_uuid', 'unknown')
            name = node.get('node_name', 'unnamed')
            address = node.get('node_address', 'no-address')
            print(f"  • {name}")
            print(f"    UUID: {uuid}")
            print(f"    Address: {address}")
            print(f"    First seen: {node.get('first_seen', 'N/A')}")
    
    # State changes timeline
    state_changes = entities.get('temporal_entities', {}).get('NodeStateChange', [])
    if state_changes:
        print(f"\nState Changes: {len(state_changes)}")
        # Show first and last
        if len(state_changes) > 0:
            first = state_changes[0]
            last = state_changes[-1]
            print(f"  First: {first.get('timestamp')} - {first.get('from_state')} → {first.get('to_state')}")
            print(f"  Last:  {last.get('timestamp')} - {last.get('from_state')} → {last.get('to_state')}")
    
    # State transfers
    state_transfers = entities.get('temporal_entities', {}).get('StateTransfer', [])
    if state_transfers:
        print(f"\nState Transfers: {len(state_transfers)}")
        for st in state_transfers[:5]:  # Show first 5
            print(f"  • {st.get('timestamp')} - {st.get('transfer_type')} ({st.get('status')})")
    
    # Errors
    errors = entities.get('temporal_entities', {}).get('ErrorEvent', [])
    if errors:
        print(f"\nErrors: {len(errors)}")
        # Count by type
        error_types = defaultdict(int)
        for error in errors:
            error_types[error.get('error_type', 'UNKNOWN')] += 1
        for error_type, count in sorted(error_types.items(), key=lambda x: -x[1]):
            print(f"  • {error_type}: {count}")


def show_statistics(stats):
    """Show processing statistics"""
    print_banner("Processing Statistics")
    
    print(f"Total lines processed: {stats['total_lines']:,}")
    print(f"Total entities extracted: {stats['total_matched']:,}")
    
    if stats['total_lines'] > 0:
        match_rate = (stats['total_matched'] / stats['total_lines']) * 100
        print(f"Match rate: {match_rate:.1f}%")
    
    print("\nPer-file statistics:")
    for filename, file_stats in stats['by_file'].items():
        lines = file_stats['lines']
        entities = file_stats['entities']
        duration = file_stats['duration']
        rate = entities / lines * 100 if lines > 0 else 0
        speed = lines / duration if duration > 0 else 0
        
        print(f"\n  {filename}:")
        print(f"    Lines: {lines:,}")
        print(f"    Entities: {entities}")
        print(f"    Match rate: {rate:.1f}%")
        print(f"    Speed: {speed:,.0f} lines/sec")


def main():
    """Main function"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    
    # Process logs
    log_dir = Path(__file__).parent / "cl407"
    
    if not log_dir.exists():
        print(f"Error: Directory not found: {log_dir}")
        return 1
    
    entities, stats = process_all_logs(log_dir)
    
    if entities is None:
        return 1
    
    # Analyze results
    analyze_entities(entities)
    
    # Show statistics
    show_statistics(stats)
    
    # Save output
    print_banner("Saving Results")
    
    output_file = Path("cl407_entities.json")
    
    try:
        with open(output_file, 'w') as f:
            json.dump(entities, f, indent=2, default=str)
        
        size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"✓ Saved to: {output_file}")
        print(f"  File size: {size_mb:.2f} MB")
        
        # Save summary
        summary_file = Path("cl407_summary.json")
        summary = {
            'statistics': stats,
            'entity_counts': {
                'core': {k: len(v) for k, v in entities.get('core_entities', {}).items()},
                'temporal': {k: len(v) for k, v in entities.get('temporal_entities', {}).items()}
            }
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"✓ Saved summary to: {summary_file}")
        
    except Exception as e:
        print(f"✗ Error saving output: {e}")
        return 1
    
    # Next steps
    print_banner("Next Steps")
    print(f"""
View full output:
  cat {output_file} | jq .

Query specific entities:
  cat {output_file} | jq '.core_entities.Node'
  cat {output_file} | jq '.temporal_entities.NodeStateChange | length'
  cat {output_file} | jq '.temporal_entities.ErrorEvent[] | .error_message'

Analyze timeline:
  cat {output_file} | jq '.temporal_entities.NodeStateChange | sort_by(.timestamp)'

Count by type:
  cat {output_file} | jq '.temporal_entities | to_entries | map({key, value: (.value | length)})'
""")
    
    print("✓ Processing complete!\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
