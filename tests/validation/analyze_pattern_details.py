#!/usr/bin/env python3
"""
Detailed Node State Pattern Analysis
Analyzes why GRAP extracts more node_state entities than raw shifting events
"""

import json
import re
import subprocess
from datetime import datetime
from collections import defaultdict

def extract_grap_patterns():
    """Extract all patterns and their usage from GRAP output"""
    print("=== ANALYZING GRAP PATTERN USAGE ===")
    
    with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
        grap_data = json.load(f)
    
    pattern_usage = defaultdict(int)
    node_state_patterns = defaultdict(list)
    
    for entity in grap_data['entities']:
        if entity['entity_type'] == 'node_state':
            pattern = entity.get('pattern_name', 'unknown')
            pattern_usage[pattern] += 1
            node_state_patterns[pattern].append({
                'timestamp': entity['timestamp'],
                'node': entity['node_name'],
                'state': entity['node_state'],
                'line_number': entity.get('line_number', 0),
                'raw_line': entity.get('raw_line', ''),
                'confidence': entity.get('confidence', 0.0)
            })
    
    print(f"\nNode state patterns found:")
    for pattern, count in sorted(pattern_usage.items()):
        print(f"  {pattern}: {count} instances")
    
    return node_state_patterns

def analyze_pattern_vs_shifting():
    """Compare GRAP patterns with raw shifting events"""
    print("\n=== PATTERN VS SHIFTING ANALYSIS ===")
    
    # Get shifting events
    files = ["cl407/error.11407.log", "cl407/error.21407.log", "cl407/error.31407.log"]
    shifting_timestamps = set()
    
    for filename in files:
        cmd = ["grep", "-i", "shifting", filename]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd="/home/claudio/Projects/GITHUB/grambo")
        
        for line in result.stdout.strip().split('\n'):
            if line and "shifting" in line.lower():
                parts = line.split()
                if len(parts) >= 2:
                    timestamp = f"{parts[0]} {parts[1]}"
                    shifting_timestamps.add((timestamp, filename))
    
    # Get GRAP patterns
    node_state_patterns = extract_grap_patterns()
    
    # Analyze which patterns are from shifting vs other sources
    for pattern_name, entities in node_state_patterns.items():
        print(f"\n--- Pattern: {pattern_name} ({len(entities)} entities) ---")
        
        shifting_matches = 0
        non_shifting_matches = 0
        
        for entity in entities[:5]:  # Show first 5 examples
            timestamp = entity['timestamp']
            raw_line = entity['raw_line']
            
            # Check if this timestamp matches a shifting event
            is_shifting = any(timestamp in ts for ts, _ in shifting_timestamps)
            
            if is_shifting or "shifting" in raw_line.lower():
                shifting_matches += 1
                print(f"  ✓ SHIFTING: {timestamp} {entity['node']} -> {entity['state']}")
            else:
                non_shifting_matches += 1
                print(f"  ? NON-SHIFTING: {timestamp} {entity['node']} -> {entity['state']}")
            
            if raw_line:
                print(f"    Raw: {raw_line[:100]}...")
        
        total_shifting = sum(1 for e in entities if "shifting" in e['raw_line'].lower())
        total_non_shifting = len(entities) - total_shifting
        print(f"  Summary: {total_shifting} shifting, {total_non_shifting} non-shifting")

def analyze_state_extraction_methods():
    """Analyze how states are extracted - shifting vs other patterns"""
    print("\n=== STATE EXTRACTION METHODS ===")
    
    with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
        grap_data = json.load(f)
    
    extraction_methods = defaultdict(list)
    
    for entity in grap_data['entities']:
        if entity['entity_type'] == 'node_state':
            raw_line = entity.get('raw_line', '').lower()
            
            if 'shifting' in raw_line:
                method = 'shifting_transition'
            elif 'state change' in raw_line:
                method = 'state_change'
            elif 'new state' in raw_line:
                method = 'new_state_declaration'
            elif 'synced' in raw_line and 'cluster' in raw_line:
                method = 'cluster_sync_message'
            elif any(word in raw_line for word in ['member', 'view', 'configuration']):
                method = 'membership_view'
            else:
                method = 'other_pattern'
            
            extraction_methods[method].append(entity)
    
    print("Extraction method distribution:")
    for method, entities in extraction_methods.items():
        print(f"  {method}: {len(entities)} entities")
        
        # Show examples
        for entity in entities[:3]:
            print(f"    Example: {entity['timestamp']} {entity['node_name']} -> {entity['node_state']}")
            if entity.get('raw_line'):
                print(f"      Raw: {entity['raw_line'][:80]}...")

def check_duplicate_extractions():
    """Check for duplicate state extractions at same timestamp"""
    print("\n=== DUPLICATE EXTRACTION ANALYSIS ===")
    
    with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
        grap_data = json.load(f)
    
    # Group by timestamp and node
    timestamp_node_groups = defaultdict(list)
    
    for entity in grap_data['entities']:
        if entity['entity_type'] == 'node_state':
            key = (entity['timestamp'], entity['node_name'])
            timestamp_node_groups[key].append(entity)
    
    # Find groups with multiple entities
    duplicates = {k: v for k, v in timestamp_node_groups.items() if len(v) > 1}
    
    print(f"Found {len(duplicates)} timestamp/node combinations with multiple state entities")
    
    # Analyze first few duplicates
    for i, ((timestamp, node), entities) in enumerate(list(duplicates.items())[:5]):
        print(f"\n{i+1}. {timestamp} - {node} ({len(entities)} entities)")
        for j, entity in enumerate(entities):
            print(f"   {j+1}) {entity['node_state']} (pattern: {entity.get('pattern_name', 'unknown')})")
            if entity.get('raw_line'):
                print(f"      Line: {entity['raw_line'][:80]}...")

def main():
    print("Detailed Node State Pattern Analysis")
    print("=" * 60)
    
    extract_grap_patterns()
    analyze_pattern_vs_shifting()
    analyze_state_extraction_methods()
    check_duplicate_extractions()
    
    print("\n=== ANALYSIS COMPLETE ===")

if __name__ == "__main__":
    main()