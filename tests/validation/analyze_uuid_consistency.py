#!/usr/bin/env python3
"""
UUID Consistency Validation
Validates that node UUIDs are consistently assigned and tracked across time
"""

import json
import sys
from collections import defaultdict
from datetime import datetime

def load_grap_output():
    """Load GRAP output and extract relevant entities"""
    try:
        with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ ERROR: grax_output/grap_output.json not found")
        sys.exit(1)

def extract_uuid_assignments():
    """Extract UUID assignments for each node over time"""
    print("=== UUID CONSISTENCY VALIDATION ===")
    
    grap_data = load_grap_output()
    
    # Track UUIDs by node and timestamp
    node_uuids = defaultdict(list)
    view_entities = []
    node_state_entities = []
    
    for entity in grap_data['entities']:
        entity_type = entity.get('entity_type', '')
        
        if entity_type == 'node_state' and 'node_uuid' in entity:
            node_state_entities.append(entity)
            node_name = entity.get('node_name', '')
            node_uuid = entity.get('node_uuid', '')
            timestamp = entity.get('timestamp', '')
            
            if node_name and node_uuid and timestamp:
                node_uuids[node_name].append({
                    'timestamp': timestamp,
                    'uuid': node_uuid,
                    'source': 'node_state'
                })
        
        elif entity_type == 'view' and 'members' in entity:
            view_entities.append(entity)
    
    return node_uuids, view_entities, node_state_entities

def validate_uuid_consistency(node_uuids):
    """Check for UUID consistency issues"""
    print(f"\n=== UUID CONSISTENCY ANALYSIS ===")
    
    issues = []
    
    for node_name, uuid_history in node_uuids.items():
        # Sort by timestamp
        uuid_history.sort(key=lambda x: x['timestamp'])
        
        print(f"\nNode: {node_name}")
        print(f"  Total UUID assignments: {len(uuid_history)}")
        
        # Check for UUID changes
        unique_uuids = set(item['uuid'] for item in uuid_history)
        print(f"  Unique UUIDs: {len(unique_uuids)}")
        
        if len(unique_uuids) > 1:
            print(f"  ⚠️  WARNING: Node has multiple UUIDs over time")
            for uuid in unique_uuids:
                count = sum(1 for item in uuid_history if item['uuid'] == uuid)
                first_seen = next(item['timestamp'] for item in uuid_history if item['uuid'] == uuid)
                print(f"    {uuid}: {count} occurrences, first seen: {first_seen}")
            
            issues.append({
                'node': node_name,
                'issue': 'multiple_uuids',
                'unique_uuids': list(unique_uuids)
            })
        else:
            print(f"  ✅ Consistent UUID: {list(unique_uuids)[0] if unique_uuids else 'None'}")
        
        # Check for temporal gaps
        if len(uuid_history) > 1:
            timestamps = [item['timestamp'] for item in uuid_history]
            # Basic gap detection (could be enhanced with datetime parsing)
            print(f"  Time span: {timestamps[0]} to {timestamps[-1]}")
    
    return issues

def validate_view_member_consistency(view_entities, node_uuids):
    """Check if view entities have consistent member UUIDs"""
    print(f"\n=== VIEW MEMBER CONSISTENCY ===")
    
    view_member_uuids = defaultdict(set)
    
    for view in view_entities:
        timestamp = view.get('timestamp', '')
        members = view.get('members', [])
        
        for member in members:
            if isinstance(member, dict):
                node_name = member.get('node_name', '')
                node_uuid = member.get('node_uuid', '')
                
                if node_name and node_uuid:
                    view_member_uuids[node_name].add(node_uuid)
    
    print(f"Found {len(view_entities)} view entities")
    print(f"Nodes in views: {list(view_member_uuids.keys())}")
    
    # Compare with node_state UUIDs
    consistency_issues = []
    
    for node_name in view_member_uuids:
        view_uuids = view_member_uuids[node_name]
        node_state_uuids = set(item['uuid'] for item in node_uuids.get(node_name, []))
        
        print(f"\nNode {node_name}:")
        print(f"  View UUIDs: {view_uuids}")
        print(f"  Node state UUIDs: {node_state_uuids}")
        
        if view_uuids != node_state_uuids:
            print(f"  ⚠️  INCONSISTENCY: View and node_state UUIDs don't match")
            consistency_issues.append({
                'node': node_name,
                'view_uuids': list(view_uuids),
                'node_state_uuids': list(node_state_uuids)
            })
        else:
            print(f"  ✅ Consistent between views and node_state")
    
    return consistency_issues

def generate_uuid_timeline():
    """Generate a timeline of UUID assignments"""
    print(f"\n=== UUID TIMELINE GENERATION ===")
    
    grap_data = load_grap_output()
    
    uuid_events = []
    for entity in grap_data['entities']:
        if entity.get('entity_type') in ['node_state', 'view'] and 'node_uuid' in entity:
            uuid_events.append({
                'timestamp': entity.get('timestamp', ''),
                'entity_type': entity.get('entity_type', ''),
                'node_name': entity.get('node_name', ''),
                'node_uuid': entity.get('node_uuid', ''),
                'source_line': entity.get('raw_line', '')[:100] + '...' if entity.get('raw_line') else ''
            })
    
    # Sort by timestamp
    uuid_events.sort(key=lambda x: x['timestamp'])
    
    print(f"Generated timeline with {len(uuid_events)} UUID-related events")
    
    # Show first few events as sample
    print("\nSample timeline entries:")
    for i, event in enumerate(uuid_events[:10]):
        print(f"  {i+1}. {event['timestamp']} - {event['node_name']} ({event['entity_type']}) -> {event['node_uuid'][:8]}...")
    
    return uuid_events

def main():
    print("UUID Consistency Validation")
    print("=" * 50)
    
    # Extract UUID data
    node_uuids, view_entities, node_state_entities = extract_uuid_assignments()
    
    print(f"Found {len(node_state_entities)} node_state entities with UUIDs")
    print(f"Found {len(view_entities)} view entities")
    
    # Validate consistency
    uuid_issues = validate_uuid_consistency(node_uuids)
    view_issues = validate_view_member_consistency(view_entities, node_uuids)
    
    # Generate timeline
    timeline = generate_uuid_timeline()
    
    # Summary
    print(f"\n=== VALIDATION SUMMARY ===")
    print(f"UUID consistency issues: {len(uuid_issues)}")
    print(f"View consistency issues: {len(view_issues)}")
    print(f"Timeline events generated: {len(timeline)}")
    
    if not uuid_issues and not view_issues:
        print("✅ UUID consistency validation PASSED")
        return 0
    else:
        print("⚠️ UUID consistency validation found issues")
        return 1

if __name__ == "__main__":
    sys.exit(main())