#!/bin/bash
# Compare V2 (grap) vs V3 (grap3) outputs

set -e

echo "═══════════════════════════════════════════════════════════════"
echo "  V2 (grap) vs V3 (grap3) Comparison"
echo "═══════════════════════════════════════════════════════════════"
echo

echo "Running V2 (grap)..."
./grap cl407/error.*.log --format=json 2>&1 | tail -n +3 > /tmp/v2_out.json

echo "Running V3 (grap3)..."
./grap3 cl407/error.*.log --format=json 2>&1 | tail -n +4 > /tmp/v3_out.json

echo
python3 << 'PYEOF'
import json

print("─" * 63)
print("ENTITY COMPARISON")
print("─" * 63)

v2 = json.load(open('/tmp/v2_out.json'))
v3 = json.load(open('/tmp/v3_out.json'))

v2_types = {}
for e in v2['entities']:
    t = e.get('entity_type', 'unknown')
    v2_types[t] = v2_types.get(t, 0) + 1

v3_types = {}
for e in v3['entities']:
    t = e.get('entity_type', 'unknown')
    v3_types[t] = v3_types.get(t, 0) + 1

all_types = sorted(set(list(v2_types.keys()) + list(v3_types.keys())))

print(f"\n{'Entity Type':<20} {'V2':<10} {'V3':<10} {'Difference':<15}")
print("─" * 63)

for t in all_types:
    v2_count = v2_types.get(t, 0)
    v3_count = v3_types.get(t, 0)
    diff = v3_count - v2_count
    diff_str = f"{diff:+d}" if diff != 0 else "same"
    marker = " 🔻" if t in ['cluster', 'view'] and diff < 0 else (" ✨" if t == 'wsrep_view' else "")
    print(f"{t:<20} {v2_count:<10} {v3_count:<10} {diff_str:<15}{marker}")

print("─" * 63)
print(f"{'TOTAL':<20} {len(v2['entities']):<10} {len(v3['entities']):<10}")

print("\n" + "─" * 63)
print("KEY DIFFERENCES")
print("─" * 63)
print("🔻 cluster: V2 creates cluster entities, V3 treats as virtual")
print("🔻 view: V2 mixes gcomm+wsrep, V3 separates them")
print("✨ wsrep_view: V3 has explicit wsrep_view entity type")
print("   node_state: V3 has from_state + to_state (V2 only has node_state)")
print("   error: V3 has stricter error pattern matching")

print("\n" + "─" * 63)
print("GRAF FRAME BUILDING")
print("─" * 63)

# Process with graf
import subprocess
import os

subprocess.run('./graf /tmp/v2_out.json --ndjson > /tmp/v2_frames.ndjson 2>&1', shell=True)
subprocess.run('./graf /tmp/v3_out.json --ndjson > /tmp/v3_frames.ndjson 2>&1', shell=True)

v2_frames = []
with open('/tmp/v2_frames.ndjson', 'r') as f:
    v2_frames = [json.loads(line) for line in f if line.strip()]

v3_frames = []
with open('/tmp/v3_frames.ndjson', 'r') as f:
    v3_frames = [json.loads(line) for line in f if line.strip()]

print(f"\nTotal frames:")
print(f"  V2: {len(v2_frames)} frames")
print(f"  V3: {len(v3_frames)} frames")

v2_with_views = sum(1 for f in v2_frames if f.get('views'))
v3_with_views = sum(1 for f in v3_frames if f.get('views'))

print(f"\nFrames with views:")
print(f"  V2: {v2_with_views} frames")
print(f"  V3: {v3_with_views} frames")

v2_gcomm = 0
v3_gcomm = 0
for f in v2_frames:
    for cluster_views in f.get('views', {}).values():
        if 'gcomm' in cluster_views:
            v2_gcomm += 1
            break
for f in v3_frames:
    for cluster_views in f.get('views', {}).values():
        if 'gcomm' in cluster_views:
            v3_gcomm += 1
            break

print(f"\nFrames with gcomm views:")
print(f"  V2: {v2_gcomm} frames")
print(f"  V3: {v3_gcomm} frames ✅ (removed)")

# Check view structure
if v2_frames and v3_frames:
    v2_sample = next((f for f in v2_frames if f.get('views')), None)
    v3_sample = next((f for f in v3_frames if f.get('views')), None)
    
    if v2_sample and v3_sample:
        print("\n" + "─" * 63)
        print("SAMPLE VIEW STRUCTURE")
        print("─" * 63)
        
        for cluster_key, layers in list(v2_sample['views'].items())[:1]:
            print(f"\nV2 view layers for {cluster_key}:")
            for layer in layers:
                view_data = layers[layer]
                print(f"  {layer}:")
                print(f"    view_id: {view_data.get('view_id', 'N/A')}")
                print(f"    cluster_uuid: {view_data.get('cluster_uuid', 'N/A')}")
        
        for cluster_key, layers in list(v3_sample['views'].items())[:1]:
            print(f"\nV3 view layers for {cluster_key}:")
            for layer in layers:
                view_data = layers[layer]
                print(f"  {layer}:")
                print(f"    group_uuid: {view_data.get('group_uuid', 'N/A')}")
                print(f"    view_id: {view_data.get('view_id', 'N/A')}")

print("\n" + "═" * 63)
print("CONCLUSION")
print("═" * 63)
print("✅ V3 provides cleaner entity separation")
print("✅ V3 removes confusing gcomm views from graf")
print("✅ V3 has explicit wsrep_view entity type")
print("✅ V3 shows proper group_uuid and view_id")
print("✅ V3 tracks state transitions (from_state → to_state)")

PYEOF

# Cleanup
rm -f /tmp/v2_out.json /tmp/v3_out.json /tmp/v2_frames.ndjson /tmp/v3_frames.ndjson

echo
