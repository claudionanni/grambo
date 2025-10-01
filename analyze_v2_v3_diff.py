#!/usr/bin/env python3
"""
Analyze differences between grap v2 and v3 outputs to identify missing patterns
"""

import json
import sys
from collections import defaultdict

def load_json(filepath):
    with open(filepath) as f:
        return json.load(f)

def main():
    v2 = load_json('grap_v2_clean.json')
    v3 = load_json('grap_v3_clean.json')
    
    print("="*80)
    print("GRAP V2 vs V3 COMPREHENSIVE COMPARISON")
    print("="*80)
    
    # 1. Overall stats
    print("\n### OVERALL STATISTICS ###")
    print(f"V2 total entities: {len(v2['entities'])}")
    print(f"V3 total entities: {len(v3['entities'])}")
    print(f"Difference: {len(v2['entities']) - len(v3['entities'])} entities missing in V3")
    
    # 2. Entity type breakdown
    v2_types = defaultdict(int)
    v3_types = defaultdict(int)
    
    for e in v2['entities']:
        v2_types[e['entity_type']] += 1
    
    for e in v3['entities']:
        v3_types[e['entity_type']] += 1
    
    print("\n### ENTITY TYPE COMPARISON ###")
    all_types = sorted(set(list(v2_types.keys()) + list(v3_types.keys())))
    
    for etype in all_types:
        v2_count = v2_types[etype]
        v3_count = v3_types[etype]
        diff = v2_count - v3_count
        status = "✓" if v3_count >= v2_count else "✗"
        print(f"{status} {etype:15s}: V2={v2_count:4d}, V3={v3_count:4d}, diff={diff:4d}")
    
    # 3. Node entity comparison
    print("\n### NODE ENTITIES ###")
    v2_nodes = [e for e in v2['entities'] if e['entity_type'] == 'node']
    v3_nodes = [e for e in v3['entities'] if e['entity_type'] == 'node']
    
    print(f"V2 nodes: {len(v2_nodes)}")
    for n in v2_nodes:
        print(f"  - {n['node_name']}: uuid_history={len(n.get('uuid_history', []))}, cluster={n.get('cluster_uuid', 'None')[:8]}")
    
    print(f"\nV3 nodes: {len(v3_nodes)}")
    for n in v3_nodes:
        print(f"  - {n['node_name']}: uuid_history={len(n.get('uuid_history', []))}, cluster={n.get('cluster_uuid', 'None')}")
    
    # 4. Cluster entity (missing in v3)
    print("\n### CLUSTER ENTITIES (MISSING IN V3) ###")
    v2_clusters = [e for e in v2['entities'] if e['entity_type'] == 'cluster']
    print(f"V2 has {len(v2_clusters)} cluster entities:")
    for c in v2_clusters:
        print(f"  - {c['entity_id']}: uuid={c.get('cluster_uuid', 'None')}")
    
    # 5. View comparison
    print("\n### VIEW ENTITIES ###")
    v2_views = [e for e in v2['entities'] if e['entity_type'] == 'view']
    v3_views = [e for e in v3['entities'] if e['entity_type'] == 'view']
    v3_wsrep_views = [e for e in v3['entities'] if e['entity_type'] == 'wsrep_view']
    
    print(f"V2 views (gcomm only): {len(v2_views)}")
    print(f"V3 gcomm views: {len(v3_views)}")
    print(f"V3 wsrep views: {len(v3_wsrep_views)}")
    print(f"V3 total views: {len(v3_views) + len(v3_wsrep_views)}")
    
    # Sample v2 view to see structure
    if v2_views:
        print("\nV2 view sample fields:")
        sample = v2_views[0]
        for k in sorted(sample.keys()):
            if k not in ['raw_line', 'validation_notes']:
                print(f"  {k}: {type(sample[k]).__name__}")
    
    # 6. SST comparison
    print("\n### SST ENTITIES ###")
    v2_sst = [e for e in v2['entities'] if e['entity_type'] == 'sst']
    v3_sst = [e for e in v3['entities'] if e['entity_type'] == 'sst']
    
    print(f"V2 SST events: {len(v2_sst)}")
    print(f"V3 SST events: {len(v3_sst)}")
    print(f"Missing: {len(v2_sst) - len(v3_sst)}")
    
    # Check unique SST event types in v2
    v2_sst_types = defaultdict(int)
    for e in v2_sst:
        event_type = e.get('event_type', 'unknown')
        v2_sst_types[event_type] += 1
    
    print("\nV2 SST event types:")
    for et, count in sorted(v2_sst_types.items()):
        print(f"  {et}: {count}")
    
    # 7. IST comparison
    print("\n### IST ENTITIES ###")
    v2_ist = [e for e in v2['entities'] if e['entity_type'] == 'ist']
    v3_ist = [e for e in v3['entities'] if e['entity_type'] == 'ist']
    
    print(f"V2 IST events: {len(v2_ist)}")
    print(f"V3 IST events: {len(v3_ist)}")
    print(f"Difference: {len(v3_ist) - len(v2_ist)} (V3 has MORE)")
    
    # 8. Error comparison
    print("\n### ERROR ENTITIES ###")
    v2_errors = [e for e in v2['entities'] if e['entity_type'] == 'error']
    v3_errors = [e for e in v3['entities'] if e['entity_type'] == 'error']
    
    print(f"V2 errors: {len(v2_errors)}")
    print(f"V3 errors: {len(v3_errors)}")
    print(f"Missing: {len(v2_errors) - len(v3_errors)}")
    
    # Check severity distribution if available
    v2_levels = defaultdict(int)
    for e in v2_errors:
        level = e.get('level', e.get('severity', 'Unknown'))
        v2_levels[level] += 1
    
    v3_levels = defaultdict(int)
    for e in v3_errors:
        level = e.get('level', e.get('severity', 'Unknown'))
        v3_levels[level] += 1
    
    print("\nV2 error levels:")
    for level, count in sorted(v2_levels.items()):
        print(f"  {level}: {count}")
    
    print("\nV3 error levels:")
    for level, count in sorted(v3_levels.items()):
        print(f"  {level}: {count}")
    
    # 9. Node state comparison
    print("\n### NODE STATE ENTITIES ###")
    v2_states = [e for e in v2['entities'] if e['entity_type'] == 'node_state']
    v3_states = [e for e in v3['entities'] if e['entity_type'] == 'node_state']
    
    print(f"V2 node states: {len(v2_states)}")
    print(f"V3 node states: {len(v3_states)}")
    print(f"Missing: {len(v2_states) - len(v3_states)}")
    
    # Check transition types in v3
    v3_transitions = defaultdict(int)
    for e in v3_states:
        tt = e.get('transition_type', 'Unknown')
        v3_transitions[tt] += 1
    
    print("\nV3 transition types:")
    for tt, count in sorted(v3_transitions.items()):
        print(f"  {tt}: {count}")
    
    # 10. Summary of action items
    print("\n" + "="*80)
    print("ACTION ITEMS FOR V3 IMPROVEMENT")
    print("="*80)
    
    print("\n1. ADD CLUSTER ENTITY TYPE")
    print("   - V2 has 3 cluster entities, V3 has none")
    print("   - Should be created at output time with aggregated statistics")
    
    print("\n2. IMPROVE ERROR DETECTION")
    print(f"   - Missing {len(v2_errors) - len(v3_errors)} error entities")
    print("   - Need to capture more Warning, ERROR, and FATAL patterns")
    
    print("\n3. IMPROVE SST DETECTION")
    print(f"   - Missing {len(v2_sst) - len(v3_sst)} SST events")
    print("   - V2 captures these event types:")
    for et in sorted(v2_sst_types.keys()):
        print(f"     * {et}")
    
    print("\n4. IMPROVE VIEW DETECTION")
    print(f"   - V2: {len(v2_views)} gcomm views")
    print(f"   - V3: {len(v3_views)} gcomm + {len(v3_wsrep_views)} wsrep = {len(v3_views) + len(v3_wsrep_views)} total")
    print("   - Consider if gcomm views are redundant with wsrep views")
    
    print("\n5. NODE STATE IMPROVEMENTS")
    print(f"   - Missing {len(v2_states) - len(v3_states)} node state changes")
    print("   - Review patterns to capture all state transitions")
    
    print("\n6. QUORUM IMPROVEMENTS")
    print(f"   - Missing {v2_types['quorum'] - v3_types['quorum']} quorum events")
    
    print("\n" + "="*80)

if __name__ == '__main__':
    main()
