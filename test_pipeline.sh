#!/bin/bash
set -e

echo "=== Testing V3 Pipeline ==="
echo

echo "Step 1: Running grap3 on cl407 logs..."
./grap3 cl407/error.*.log --format=json 2>&1 | tail -n +4 > test_pipeline_grap3.json
echo "✓ grap3 output: $(wc -l < test_pipeline_grap3.json) lines"

echo
echo "Step 2: Running graf to build frames..."
./graf test_pipeline_grap3.json --ndjson > test_pipeline_frames.ndjson 2>&1
echo "✓ graf output: $(wc -l < test_pipeline_frames.ndjson) frames"

echo
echo "Step 3: Analyzing output..."
python3 << 'PYEOF'
import json

# Load grap3 output
with open('test_pipeline_grap3.json', 'r') as f:
    grap_data = json.load(f)

print(f"GRAP3 Stats:")
print(f"  Total entities: {len(grap_data['entities'])}")

entity_types = {}
for e in grap_data['entities']:
    et = e.get('entity_type', 'unknown')
    entity_types[et] = entity_types.get(et, 0) + 1

print(f"  Entity types:")
for et, count in sorted(entity_types.items()):
    print(f"    {et}: {count}")

# Check wsrep_view entities
wsrep_views = [e for e in grap_data['entities'] if e.get('entity_type') == 'wsrep_view']
if wsrep_views:
    sample = wsrep_views[0]
    print(f"\n  Sample wsrep_view fields:")
    print(f"    group_uuid: {sample.get('group_uuid')}")
    print(f"    view_id: {sample.get('view_id')}")
    print(f"    view_seq: {sample.get('view_seq')}")
    print(f"    timestamp: {sample.get('timestamp')}")

# Load frames
with open('test_pipeline_frames.ndjson', 'r') as f:
    frames = [json.loads(line) for line in f if line.strip()]

print(f"\nGRAF Stats:")
print(f"  Total frames: {len(frames)}")

frames_with_views = [f for f in frames if f.get('views')]
print(f"  Frames with views: {len(frames_with_views)}")

# Check for gcomm views (should be 0)
gcomm_count = sum(1 for f in frames for cluster_views in f.get('views', {}).values() if 'gcomm' in cluster_views)
print(f"  Frames with gcomm views: {gcomm_count}")

if frames_with_views:
    sample_frame = frames_with_views[0]
    print(f"\n  Sample frame {sample_frame['index']} views:")
    for cluster_key, layers in sample_frame['views'].items():
        for layer, props in layers.items():
            print(f"    {cluster_key} / {layer}:")
            print(f"      group_uuid: {props.get('group_uuid')}")
            print(f"      view_id: {props.get('view_id')}")

# Check node states have from_state and to_state
node_states_with_transition = []
for f in frames[:100]:  # Check first 100 frames
    for node_name, node_props in f.get('nodes', {}).items():
        if 'from_state' in node_props and 'to_state' in node_props:
            node_states_with_transition.append(node_props)

print(f"\n  Nodes with state transitions: {len(node_states_with_transition)}")
if node_states_with_transition:
    sample_node = node_states_with_transition[0]
    print(f"    Sample: {sample_node.get('from_state')} -> {sample_node.get('to_state')}")

PYEOF

echo
echo "=== Pipeline Test Complete ==="
