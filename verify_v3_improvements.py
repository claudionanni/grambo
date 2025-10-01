#!/usr/bin/env python3
"""
Verify V3-alpha improvements

This script validates that all v3-alpha improvements are working correctly:
1. Views filtered by timestamp
2. from_state/to_state present in node transitions
3. wsrep_view entities properly integrated
4. Node UUID history tracked
"""

import json
import sys
from datetime import datetime
from typing import Dict, List, Any

def parse_iso(ts: str) -> datetime:
    """Parse ISO timestamp"""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None

def test_view_filtering(frames: List[Dict[str, Any]]) -> bool:
    """Test that views are filtered by timestamp"""
    print("\n1. Testing View Timestamp Filtering")
    print("-" * 70)
    
    errors = []
    checked = 0
    
    for frame in frames[::50]:  # Check every 50th frame
        event_ts_str = frame.get('event', {}).get('timestamp')
        if not event_ts_str:
            continue
            
        event_ts = parse_iso(event_ts_str)
        if not event_ts:
            continue
        
        views = frame.get('views', {})
        for cluster_key, layers in views.items():
            for layer_name, layer_data in layers.items():
                view_ts_str = layer_data.get('timestamp')
                if not view_ts_str:
                    continue
                    
                view_ts = parse_iso(view_ts_str)
                if not view_ts:
                    continue
                
                checked += 1
                if view_ts > event_ts:
                    errors.append(f"Frame {frame.get('index')}: {cluster_key}/{layer_name} "
                                f"view_ts={view_ts_str} > event_ts={event_ts_str}")
    
    if errors:
        print(f"✗ FAILED: Found {len(errors)} views with future timestamps:")
        for err in errors[:5]:
            print(f"  {err}")
        return False
    else:
        print(f"✓ PASSED: All {checked} views have timestamps ≤ frame timestamp")
        return True

def test_state_transitions(frames: List[Dict[str, Any]]) -> bool:
    """Test that from_state and to_state are present"""
    print("\n2. Testing State Transitions (from_state/to_state)")
    print("-" * 70)
    
    transitions = []
    
    for frame in frames:
        nodes = frame.get('nodes', {})
        for node_name, node_data in nodes.items():
            if 'from_state' in node_data and 'to_state' in node_data:
                from_s = node_data.get('from_state')
                to_s = node_data.get('to_state')
                node_s = node_data.get('node_state')
                
                transitions.append({
                    'frame': frame.get('index'),
                    'node': node_name,
                    'from_state': from_s,
                    'to_state': to_s,
                    'node_state': node_s,
                    'matches': to_s == node_s
                })
                break
    
    if not transitions:
        print("✗ FAILED: No state transitions found")
        return False
    
    # Check that node_state matches to_state
    mismatches = [t for t in transitions if not t['matches']]
    if mismatches:
        print(f"✗ FAILED: Found {len(mismatches)} mismatches between to_state and node_state:")
        for t in mismatches[:3]:
            print(f"  Frame {t['frame']}: {t['node']} to_state={t['to_state']} != node_state={t['node_state']}")
        return False
    
    print(f"✓ PASSED: Found {len(transitions)} state transitions")
    print(f"  All transitions have matching to_state and node_state")
    
    # Show sample transitions
    print("\n  Sample transitions:")
    for t in transitions[:5]:
        print(f"    Frame {t['frame']:3d}: {t['node']:12s} {t['from_state']:8s} → {t['to_state']:8s}")
    
    return True

def test_wsrep_views(frames: List[Dict[str, Any]]) -> bool:
    """Test wsrep_view entities"""
    print("\n3. Testing wsrep_view Entity Integration")
    print("-" * 70)
    
    wsrep_event_count = 0
    wsrep_layer_count = 0
    
    # Count wsrep_view events
    for frame in frames:
        event = frame.get('event', {})
        if event.get('entity_type') == 'wsrep_view':
            wsrep_event_count += 1
        
        # Count wsrep view layers in frames
        views = frame.get('views', {})
        for cluster_key, layers in views.items():
            if 'wsrep' in layers:
                wsrep_layer_count += 1
                break
    
    if wsrep_event_count == 0:
        print("✗ FAILED: No wsrep_view events found")
        return False
    
    if wsrep_layer_count == 0:
        print("✗ FAILED: No wsrep view layers found in frames")
        return False
    
    print(f"✓ PASSED: wsrep_view integration working")
    print(f"  wsrep_view events: {wsrep_event_count}")
    print(f"  Frames with wsrep layer: {wsrep_layer_count}")
    
    # Show sample wsrep_view
    for frame in frames:
        event = frame.get('event', {})
        if event.get('entity_type') == 'wsrep_view':
            views = frame.get('views', {})
            for cluster_key, layers in views.items():
                if 'wsrep' in layers:
                    wsrep = layers['wsrep']
                    print(f"\n  Sample wsrep_view (Frame {frame.get('index')}):")
                    print(f"    view_id: {wsrep.get('view_id')}")
                    print(f"    status: {wsrep.get('status')}")
                    print(f"    members: {wsrep.get('member_count', 0)}")
                    print(f"    timestamp: {wsrep.get('timestamp')}")
                    break
            break
    
    return True

def test_node_uuid_history(frames: List[Dict[str, Any]]) -> bool:
    """Test node UUID history tracking"""
    print("\n4. Testing Node UUID History")
    print("-" * 70)
    
    nodes_with_history = {}
    
    # Find nodes with UUID history
    for frame in frames:
        nodes = frame.get('nodes', {})
        for node_name, node_data in nodes.items():
            if 'uuid_history' in node_data:
                history = node_data.get('uuid_history', [])
                if history and node_name not in nodes_with_history:
                    nodes_with_history[node_name] = history
    
    if not nodes_with_history:
        print("✗ FAILED: No nodes with UUID history found")
        return False
    
    print(f"✓ PASSED: Node UUID history tracking working")
    print(f"  Nodes tracked: {len(nodes_with_history)}")
    
    for node_name, history in nodes_with_history.items():
        # Count full UUIDs (5 parts with dashes)
        full_uuids = [u for u in history if str(u).count('-') == 4]
        print(f"\n  {node_name}:")
        print(f"    Total history entries: {len(history)}")
        print(f"    Full UUIDs: {len(full_uuids)}")
        if full_uuids:
            print(f"    Sample: {full_uuids[0]}")
    
    return True

def main():
    """Run all verification tests"""
    print("=" * 70)
    print("V3-Alpha Improvements Verification")
    print("=" * 70)
    
    # Load graf output
    try:
        with open('graffed3.json') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("\n✗ ERROR: graffed3.json not found")
        print("  Run: ./grap3 cl407/error.*.log --format=json | ./graf > graffed3.json")
        return 1
    except json.JSONDecodeError as e:
        print(f"\n✗ ERROR: Invalid JSON in graffed3.json: {e}")
        return 1
    
    frames = data.get('frames', [])
    if not frames:
        print("\n✗ ERROR: No frames found in graffed3.json")
        return 1
    
    print(f"\nLoaded {len(frames)} frames from graffed3.json")
    
    # Run all tests
    results = []
    results.append(test_view_filtering(frames))
    results.append(test_state_transitions(frames))
    results.append(test_wsrep_views(frames))
    results.append(test_node_uuid_history(frames))
    
    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"\n✓ ALL TESTS PASSED ({passed}/{total})")
        print("\nV3-alpha improvements verified successfully!")
        return 0
    else:
        print(f"\n✗ SOME TESTS FAILED ({passed}/{total} passed)")
        return 1

if __name__ == '__main__':
    sys.exit(main())
