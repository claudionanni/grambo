#!/usr/bin/env python3
"""
Node State Reliability Analysis
Compares raw log "shifting" events with GRAP extracted node_state entities
"""

import json
import re
import subprocess
from datetime import datetime
from collections import defaultdict

def extract_raw_shifting_events():
    """Extract state transitions from raw log files"""
    print("=== EXTRACTING RAW LOG SHIFTING EVENTS ===")
    
    # Get raw shifting events from each file
    files = ["cl407/error.11407.log", "cl407/error.21407.log", "cl407/error.31407.log"]
    
    raw_events = []
    for filename in files:
        try:
            cmd = ["grep", "-i", "shifting", filename]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd="/home/claudio/Projects/GITHUB/grambo")
            
            for line in result.stdout.strip().split('\n'):
                if line and "shifting" in line.lower():
                    # Parse: timestamp ... WSREP: Shifting FROM -> TO (TO: n)
                    parts = line.split()
                    if len(parts) >= 10:
                        timestamp = f"{parts[0]} {parts[1]}"
                        
                        # Find the shifting part
                        shifting_idx = -1
                        for i, part in enumerate(parts):
                            if part.lower() == "shifting":
                                shifting_idx = i
                                break
                        
                        if shifting_idx != -1 and shifting_idx + 3 < len(parts):
                            from_state = parts[shifting_idx + 1]
                            to_state = parts[shifting_idx + 3]
                            
                            # Determine node from filename
                            if "11407" in filename:
                                node = "NODE_11407"
                            elif "21407" in filename:
                                node = "NODE_21407"
                            elif "31407" in filename:
                                node = "NODE_31407"
                            else:
                                continue
                            
                            raw_events.append({
                                'timestamp': timestamp,
                                'node': node,
                                'from_state': from_state,
                                'to_state': to_state,
                                'filename': filename,
                                'raw_line': line
                            })
                            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    print(f"Found {len(raw_events)} raw shifting events")
    return raw_events

def extract_grap_node_states():
    """Extract node_state entities from GRAP output"""
    print("\n=== EXTRACTING GRAP NODE_STATE ENTITIES ===")
    
    with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
        grap_data = json.load(f)
    
    node_states = []
    for entity in grap_data['entities']:
        if entity['entity_type'] == 'node_state':
            node_states.append({
                'timestamp': entity['timestamp'],
                'node': entity['node_name'],
                'state': entity['node_state'],
                'line_number': entity.get('line_number', 0),
                'log_source': entity.get('log_source', ''),
                'confidence': entity.get('confidence', 0.0)
            })
    
    print(f"Found {len(node_states)} GRAP node_state entities")
    return node_states

def analyze_state_transitions(raw_events, grap_states):
    """Analyze and compare state transitions"""
    print("\n=== ANALYZING STATE TRANSITIONS ===")
    
    # Group by timestamp and node
    raw_by_time_node = defaultdict(list)
    grap_by_time_node = defaultdict(list)
    
    for event in raw_events:
        key = (event['timestamp'], event['node'])
        raw_by_time_node[key].append(event)
    
    for state in grap_states:
        key = (state['timestamp'], state['node'])
        grap_by_time_node[key].append(state)
    
    print(f"\nRaw events grouped into {len(raw_by_time_node)} timestamp/node combinations")
    print(f"GRAP states grouped into {len(grap_by_time_node)} timestamp/node combinations")
    
    # Compare coverage
    raw_keys = set(raw_by_time_node.keys())
    grap_keys = set(grap_by_time_node.keys())
    
    print(f"\nMatching timestamp/node combinations: {len(raw_keys & grap_keys)}")
    print(f"Raw-only combinations: {len(raw_keys - grap_keys)}")
    print(f"GRAP-only combinations: {len(grap_keys - raw_keys)}")
    
    return raw_by_time_node, grap_by_time_node

def detailed_comparison(raw_by_time_node, grap_by_time_node):
    """Detailed comparison of specific transitions"""
    print("\n=== DETAILED COMPARISON ===")
    
    # Sample some matching events for detailed analysis
    matching_keys = list(set(raw_by_time_node.keys()) & set(grap_by_time_node.keys()))
    
    print(f"\nAnalyzing first 10 matching events:")
    for i, key in enumerate(sorted(matching_keys)[:10]):
        timestamp, node = key
        raw_events = raw_by_time_node[key]
        grap_states = grap_by_time_node[key]
        
        print(f"\n{i+1}. {timestamp} - {node}")
        print(f"   Raw events: {len(raw_events)}")
        for event in raw_events:
            print(f"     {event['from_state']} -> {event['to_state']}")
        
        print(f"   GRAP states: {len(grap_states)}")
        for state in grap_states:
            print(f"     {state['state']} (confidence: {state['confidence']})")

def node_state_statistics(raw_events, grap_states):
    """Generate statistics on node state detection"""
    print("\n=== NODE STATE STATISTICS ===")
    
    # Count states by node
    raw_node_counts = defaultdict(int)
    grap_node_counts = defaultdict(int)
    
    for event in raw_events:
        raw_node_counts[event['node']] += 1
    
    for state in grap_states:
        grap_node_counts[state['node']] += 1
    
    print("\nState transition counts by node:")
    all_nodes = set(raw_node_counts.keys()) | set(grap_node_counts.keys())
    for node in sorted(all_nodes):
        raw_count = raw_node_counts.get(node, 0)
        grap_count = grap_node_counts.get(node, 0)
        print(f"  {node}: Raw={raw_count}, GRAP={grap_count}, Ratio={grap_count/raw_count if raw_count > 0 else 'N/A'}")

def main():
    print("Node State Reliability Analysis")
    print("=" * 50)
    
    # Extract data
    raw_events = extract_raw_shifting_events()
    grap_states = extract_grap_node_states()
    
    # Analyze
    raw_by_time_node, grap_by_time_node = analyze_state_transitions(raw_events, grap_states)
    detailed_comparison(raw_by_time_node, grap_by_time_node)
    node_state_statistics(raw_events, grap_states)
    
    print("\n=== ANALYSIS COMPLETE ===")

if __name__ == "__main__":
    main()