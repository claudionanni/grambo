#!/usr/bin/env python3
"""
Node Mapping Checker
====================

This script helps detect when manual node name specification is needed
for grambo-cluster.py by analyzing potential conflicts in node identification.
"""

import json
import sys
from collections import defaultdict

def check_node_mapping(json_files):
    """Check if manual node mapping is needed"""
    print("🔍 CHECKING NODE MAPPING REQUIREMENTS")
    print("=" * 50)
    
    file_data = []
    all_local_names = []
    all_visible_nodes = set()
    
    # Load each file and extract node information
    for file_path in json_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract local node name
            local_name = data.get('cluster_info', {}).get('local_node_name')
            
            # Extract all visible nodes
            visible_nodes = set()
            if 'nodes' in data:
                visible_nodes.update(data['nodes'].keys())
            
            file_data.append({
                'file': file_path,
                'local_name': local_name,
                'visible_nodes': visible_nodes
            })
            
            all_local_names.append(local_name)
            all_visible_nodes.update(visible_nodes)
            
            print(f"📁 {file_path}:")
            print(f"   Local node name: {local_name or 'MISSING'}")
            print(f"   Visible nodes: {sorted(visible_nodes) if visible_nodes else 'NONE'}")
            print()
            
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            continue
    
    # Analyze for conflicts
    print("🧐 ANALYSIS:")
    print("-" * 30)
    
    # Check for duplicate local names
    local_name_counts = defaultdict(int)
    for name in all_local_names:
        if name:
            local_name_counts[name] += 1
    
    duplicates = {name: count for name, count in local_name_counts.items() if count > 1}
    
    if duplicates:
        print("⚠️  CONFLICT DETECTED: Duplicate local node names!")
        for name, count in duplicates.items():
            print(f"   '{name}' appears in {count} files")
        print()
        print("🔧 SOLUTION: Use manual node mapping with --node")
        print("   Example:")
        
        # Generate suggested mapping based on filename hints and visible nodes
        unique_nodes = sorted([n for n in all_visible_nodes if n != 'offset'])
        for i, file_info in enumerate(file_data):
            if i < len(unique_nodes):
                # Try to guess from filename
                filename = file_info['file']
                if 'd01' in filename.lower() and 'vinfr-db-d-d01' in unique_nodes:
                    suggested_name = 'vinfr-db-d-d01'
                elif 'l05' in filename.lower() and 'vinfr-db-d-l05' in unique_nodes:
                    suggested_name = 'vinfr-db-d-l05'
                else:
                    suggested_name = unique_nodes[i]
                print(f"   --node {suggested_name}:{file_info['file']}")
        
        print("\n   Full command:")
        cmd_parts = ["python3 grambo-cluster.py"]
        for i, file_info in enumerate(file_data):
            if i < len(unique_nodes):
                filename = file_info['file']
                if 'd01' in filename.lower() and 'vinfr-db-d-d01' in unique_nodes:
                    suggested_name = 'vinfr-db-d-d01'
                elif 'l05' in filename.lower() and 'vinfr-db-d-l05' in unique_nodes:
                    suggested_name = 'vinfr-db-d-l05'
                else:
                    suggested_name = unique_nodes[i]
                cmd_parts.append(f"--node {suggested_name}:{file_info['file']}")
        cmd_parts.append("--format=json")
        print(f"   {' '.join(cmd_parts)}")
        
        return True  # Manual mapping needed
        
    elif len(set(all_local_names)) == len(json_files):
        print("✅ NO CONFLICTS: Each file has a unique local node name")
        print("   Automatic mapping should work fine")
        return False  # No manual mapping needed
        
    else:
        print("⚠️  PARTIAL CONFLICT: Some files missing local node names")
        print("🔧 RECOMMENDATION: Consider using manual node mapping")
        return True  # Manual mapping recommended

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 check-node-mapping.py file1.json file2.json ...")
        sys.exit(1)
    
    needs_manual = check_node_mapping(sys.argv[1:])
    
    if needs_manual:
        print("\n📋 SUMMARY: Manual node mapping RECOMMENDED")
        sys.exit(1)
    else:
        print("\n📋 SUMMARY: Automatic mapping should work")
        sys.exit(0)
