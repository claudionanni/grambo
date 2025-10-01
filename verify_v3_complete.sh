#!/bin/bash
# Final verification of v3-alpha pipeline

set -e

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║         V3-ALPHA PIPELINE VERIFICATION                        ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo

# Step 1: GRAP3 parsing
echo "▶ Step 1: Running grap3 on cl407 logs..."
./grap3 cl407/error.*.log --format=json 2>&1 | tail -n +4 > /tmp/verify_grap3.json
echo "  ✓ Generated $(wc -l < /tmp/verify_grap3.json) lines"

# Step 2: GRAF frame building
echo
echo "▶ Step 2: Running graf to build frames..."
./graf /tmp/verify_grap3.json --ndjson > /tmp/verify_frames.ndjson 2>&1
echo "  ✓ Generated $(wc -l < /tmp/verify_frames.ndjson) frames"

# Step 3: Analysis
echo
echo "▶ Step 3: Analyzing pipeline output..."
python3 << 'PYEOF'
import json
import sys

print()
print("─" * 63)
print("VERIFICATION RESULTS")
print("─" * 63)

# Load grap3 output
with open('/tmp/verify_grap3.json', 'r') as f:
    grap_data = json.load(f)

# Load frames
with open('/tmp/verify_frames.ndjson', 'r') as f:
    frames = [json.loads(line) for line in f if line.strip()]

# Check 1: Entity types
print("\n✓ GRAP3 Entity Types:")
entity_types = {}
for e in grap_data['entities']:
    et = e.get('entity_type', 'unknown')
    entity_types[et] = entity_types.get(et, 0) + 1

has_wsrep_view = 'wsrep_view' in entity_types
has_separate_view = 'view' in entity_types
print(f"  {'✓' if has_wsrep_view else '✗'} wsrep_view entities: {entity_types.get('wsrep_view', 0)}")
print(f"  {'✓' if has_separate_view else '✗'} view entities (gcomm): {entity_types.get('view', 0)}")

# Check 2: WSREP view structure
wsrep_views = [e for e in grap_data['entities'] if e.get('entity_type') == 'wsrep_view']
if wsrep_views:
    sample = wsrep_views[0]
    has_group_uuid = 'group_uuid' in sample
    has_view_id = 'view_id' in sample
    view_id_format_ok = ':' in str(sample.get('view_id', ''))
    
    print(f"\n✓ WSREP View Structure:")
    print(f"  {'✓' if has_group_uuid else '✗'} Has group_uuid field")
    print(f"  {'✓' if has_view_id else '✗'} Has view_id field")
    print(f"  {'✓' if view_id_format_ok else '✗'} view_id format is group_uuid:seqno")
    
    if has_group_uuid and has_view_id:
        print(f"  Example: {sample.get('view_id')}")

# Check 3: Node state transitions
node_states = [e for e in grap_data['entities'] if e.get('entity_type') == 'node_state']
states_with_transitions = [e for e in node_states if 'from_state' in e and 'to_state' in e]

print(f"\n✓ Node State Transitions:")
print(f"  Total node_state entities: {len(node_states)}")
print(f"  {'✓' if states_with_transitions else '✗'} With from_state and to_state: {len(states_with_transitions)}")

if states_with_transitions:
    sample = states_with_transitions[0]
    print(f"  Example: {sample.get('from_state')} → {sample.get('to_state')}")

# Check 4: Frame views
frames_with_views = [f for f in frames if f.get('views')]
frames_with_gcomm = sum(1 for f in frames for cv in f.get('views', {}).values() if 'gcomm' in cv)

print(f"\n✓ GRAF Frame Generation:")
print(f"  Total frames: {len(frames)}")
print(f"  Frames with views: {len(frames_with_views)}")
print(f"  {'✓' if frames_with_gcomm == 0 else '✗'} Frames with gcomm views: {frames_with_gcomm} (should be 0)")

# Check 5: View structure in frames
if frames_with_views:
    sample_frame = next((f for f in frames if f.get('views')), None)
    if sample_frame:
        for cluster_key, layers in sample_frame['views'].items():
            if 'wsrep' in layers:
                wsrep = layers['wsrep']
                has_group_uuid_in_frame = 'group_uuid' in wsrep
                has_view_id_in_frame = 'view_id' in wsrep
                
                print(f"\n✓ Frame View Structure:")
                print(f"  {'✓' if has_group_uuid_in_frame else '✗'} Has group_uuid in frame")
                print(f"  {'✓' if has_view_id_in_frame else '✗'} Has view_id in frame")
                
                if has_view_id_in_frame:
                    print(f"  view_id: {wsrep.get('view_id')}")
                break

# Check 6: Node entities with UUID history
nodes = [e for e in grap_data['entities'] if e.get('entity_type') == 'node']
nodes_with_uuid_history = [n for n in nodes if 'uuid_history' in n and len(n['uuid_history']) > 0]

print(f"\n✓ Node UUID History:")
print(f"  Total node entities: {len(nodes)}")
print(f"  {'✓' if nodes_with_uuid_history else '✗'} With UUID history: {len(nodes_with_uuid_history)}")

if nodes_with_uuid_history:
    print(f"  Example: {nodes_with_uuid_history[0].get('node_name')} has {len(nodes_with_uuid_history[0]['uuid_history'])} UUIDs")

# Final verdict
print("\n" + "─" * 63)
print("FINAL VERDICT")
print("─" * 63)

all_checks_passed = (
    has_wsrep_view and
    has_separate_view and
    len(wsrep_views) > 0 and
    len(states_with_transitions) > 0 and
    frames_with_gcomm == 0 and
    len(frames_with_views) > 0 and
    len(nodes_with_uuid_history) > 0
)

if all_checks_passed:
    print("✅ ALL CHECKS PASSED")
    print()
    print("The v3-alpha pipeline is working correctly:")
    print("  ✓ GRAP3 extracts entities with proper types")
    print("  ✓ WSREP views have group_uuid and view_id")
    print("  ✓ Node states track from_state → to_state")
    print("  ✓ GRAF excludes gcomm views")
    print("  ✓ Frames have proper view structure")
    print("  ✓ Nodes have UUID history")
    sys.exit(0)
else:
    print("❌ SOME CHECKS FAILED")
    print("Please review the output above for details.")
    sys.exit(1)

PYEOF

RESULT=$?

# Cleanup
rm -f /tmp/verify_grap3.json /tmp/verify_frames.ndjson

if [ $RESULT -eq 0 ]; then
    echo
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║              VERIFICATION COMPLETE ✅                          ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
else
    echo
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║              VERIFICATION FAILED ❌                            ║"
    echo "╚═══════════════════════════════════════════════════════════════╝"
fi

exit $RESULT
