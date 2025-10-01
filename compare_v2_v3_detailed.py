#!/usr/bin/env python3
"""
Detailed comparison between GRAP v2 and v3 outputs
Focuses on identifying missing features and field mappings
"""
import json
import sys
from collections import defaultdict

def load_json(filename):
    with open(filename) as f:
        return json.load(f)

def main():
    v2 = load_json('grap_v2_clean.json')
    v3 = load_json('grap_v3_clean.json')
    
    print("=" * 80)
    print("COMPREHENSIVE V2 vs V3 COMPARISON")
    print("=" * 80)
    
    # 1. NODE ENTITY COMPARISON
    print("\n### 1. NODE ENTITY COMPARISON ###\n")
    
    node_v2 = next((e for e in v2['entities'] if e.get('entity_type') == 'node'), None)
    node_v3 = next((e for e in v3['entities'] if e.get('entity_type') == 'node'), None)
    
    if node_v2 and node_v3:
        v2_fields = set(node_v2.keys())
        v3_fields = set(node_v3.keys())
        
        print(f"V2 fields: {sorted(v2_fields)}")
        print(f"\nV3 fields: {sorted(v3_fields)}")
        
        missing_in_v3 = v2_fields - v3_fields
        new_in_v3 = v3_fields - v2_fields
        
        if missing_in_v3:
            print(f"\n⚠️  Missing in V3: {sorted(missing_in_v3)}")
        if new_in_v3:
            print(f"\n✨ New in V3: {sorted(new_in_v3)}")
        
        # Check UUID history
        print(f"\nV2 UUID history length: {len(node_v2.get('uuid_history', []))}")
        print(f"V3 UUID history length: {len(node_v3.get('uuid_history', []))}")
        print(f"V2 long_uuid: {node_v2.get('long_uuid')}")
        print(f"V3 long_uuid: {node_v3.get('long_uuid')}")
        
    # 2. WSREP VIEW COMPARISON
    print("\n\n### 2. WSREP VIEW COMPARISON ###\n")
    
    # Find wsrep views in v2 (entity_type=view with view_layer=wsrep)
    wsrep_v2 = next((e for e in v2['entities'] 
                     if e.get('entity_type') == 'view' and e.get('view_layer') == 'wsrep'), None)
    wsrep_v3 = next((e for e in v3['entities'] if e.get('entity_type') == 'wsrep_view'), None)
    
    if wsrep_v2:
        print("V2 WSREP VIEW STRUCTURE (entity_type='view'):")
        print(json.dumps({k: v for k, v in wsrep_v2.items() if k != 'raw_line'}, indent=2))
    
    if wsrep_v3:
        print("\nV3 WSREP VIEW STRUCTURE (entity_type='wsrep_view'):")
        print(json.dumps({k: v for k, v in wsrep_v3.items() if k != 'raw_line'}, indent=2))
    
    if wsrep_v2 and wsrep_v3:
        v2_fields = set(wsrep_v2.keys())
        v3_fields = set(wsrep_v3.keys())
        
        missing_in_v3 = v2_fields - v3_fields
        new_in_v3 = v3_fields - v2_fields
        
        if missing_in_v3:
            print(f"\n⚠️  Missing in V3: {sorted(missing_in_v3)}")
        if new_in_v3:
            print(f"\n✨ New in V3: {sorted(new_in_v3)}")
    
    # 3. NODE STATE COMPARISON
    print("\n\n### 3. NODE STATE COMPARISON ###\n")
    
    ns_v2 = next((e for e in v2['entities'] if e.get('entity_type') == 'node_state'), None)
    ns_v3 = next((e for e in v3['entities'] if e.get('entity_type') == 'node_state'), None)
    
    if ns_v2:
        print("V2 NODE STATE:")
        print(json.dumps({k: v for k, v in ns_v2.items() if k != 'raw_line'}, indent=2))
    
    if ns_v3:
        print("\nV3 NODE STATE:")
        print(json.dumps({k: v for k, v in ns_v3.items() if k != 'raw_line'}, indent=2))
    
    if ns_v2 and ns_v3:
        v2_fields = set(ns_v2.keys())
        v3_fields = set(ns_v3.keys())
        
        missing_in_v3 = v2_fields - v3_fields
        new_in_v3 = v3_fields - v2_fields
        
        if missing_in_v3:
            print(f"\n⚠️  Missing in V3: {sorted(missing_in_v3)}")
        if new_in_v3:
            print(f"\n✨ New in V3: {sorted(new_in_v3)}")
    
    # 4. ENTITY COUNT SUMMARY
    print("\n\n### 4. ENTITY COUNT SUMMARY ###\n")
    
    v2_counts = defaultdict(int)
    for e in v2['entities']:
        v2_counts[e.get('entity_type')] += 1
    
    v3_counts = defaultdict(int)
    for e in v3['entities']:
        v3_counts[e.get('entity_type')] += 1
    
    all_types = sorted(set(v2_counts.keys()) | set(v3_counts.keys()))
    
    print(f"{'Entity Type':<20} {'V2 Count':>10} {'V3 Count':>10} {'Diff':>10}")
    print("-" * 55)
    for et in all_types:
        v2c = v2_counts.get(et, 0)
        v3c = v3_counts.get(et, 0)
        diff = v3c - v2c
        symbol = "✓" if diff >= 0 else "⚠"
        print(f"{et:<20} {v2c:>10} {v3c:>10} {diff:>9d} {symbol}")
    
    # 5. KEY FINDINGS AND RECOMMENDATIONS
    print("\n\n### 5. KEY FINDINGS AND RECOMMENDATIONS ###\n")
    
    findings = []
    
    # Check for missing entity types
    if v2_counts.get('view', 0) > v3_counts.get('view', 0) + v3_counts.get('wsrep_view', 0):
        findings.append("⚠️  V3 has fewer total views than V2")
    
    # Check node state differences
    if v2_counts.get('node_state', 0) != v3_counts.get('node_state', 0):
        findings.append(f"⚠️  Node state count differs: V2={v2_counts.get('node_state', 0)}, V3={v3_counts.get('node_state', 0)}")
    
    # Check for v2 -> v3 field mapping
    if node_v2 and node_v3:
        if node_v2.get('node_id') != node_v3.get('node_id'):
            findings.append(f"⚠️  Node ID mismatch: V2={node_v2.get('node_id')}, V3={node_v3.get('node_id')}")
    
    if ns_v2 and ns_v3:
        if 'node_state' in ns_v2 and 'to_state' in ns_v3:
            findings.append("✓ V3 uses from_state/to_state instead of V2's node_state")
        if 'node_state' not in ns_v2 and 'to_state' not in ns_v3:
            findings.append("⚠️  Neither V2 nor V3 have expected state fields")
    
    # Check wsrep view implementation
    if v3_counts.get('wsrep_view', 0) > 0:
        findings.append("✓ V3 successfully separates wsrep_view from gcomm view")
    
    if findings:
        for f in findings:
            print(f)
    else:
        print("✓ No critical issues found")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    main()
