#!/usr/bin/env python3

import json
from datetime import datetime

# Load the entities
with open('debug_entities.json', 'r') as f:
    data = json.load(f)

entities = data['entities']
sst_entities = [e for e in entities if e.get('entity_type') == 'sst']
cluster_entities = [e for e in entities if e.get('entity_type') == 'cluster']

print(f"Total entities: {len(entities)}")
print(f"SST entities: {len(sst_entities)}")
print(f"Cluster entities: {len(cluster_entities)}")

if cluster_entities:
    cluster = cluster_entities[0]
    cluster_uuid = cluster.get('cluster_uuid', '')
    cluster_ref = f"cluster_{cluster_uuid[:8]}"
    
    print(f"\nCluster UUID: {cluster_uuid}")
    print(f"Expected cluster_ref: {cluster_ref}")
    
    # Test the filtering logic from _calculate_cluster_statistics
    # Get the period (for single cluster, period is start_time=None, end_time=None)
    start_time = None  # No time filtering for single cluster
    end_time = None
    
    # Filter entities that belong to this cluster period
    cluster_entities_filtered = [
        e for e in entities 
        if e.get('cluster_ref', '') == cluster_ref
    ]
    
    print(f"\nEntities with matching cluster_ref: {len(cluster_entities_filtered)}")
    
    # Check SST entities specifically
    sst_in_cluster = [
        e for e in cluster_entities_filtered 
        if e.get('entity_type') == 'sst'
    ]
    print(f"SST entities in cluster: {len(sst_in_cluster)}")
    
    # Count SST initiation events (request or donor_selected)
    sst_initiation_events = [
        e for e in sst_in_cluster 
        if e.get('event_type') in ('request', 'donor_selected')
    ]
    print(f"SST initiation events (request or donor_selected): {len(sst_initiation_events)}")
    
    for sst in sst_initiation_events[:5]:
        print(f"  Line {sst.get('line_number')}: {sst.get('event_type')} - cluster_ref: {sst.get('cluster_ref')}")
    
    print(f"\nExpected SST count: {len(sst_initiation_events)}")
    print(f"Actual cluster total_sst_operations: {cluster.get('total_sst_operations', 0)}")