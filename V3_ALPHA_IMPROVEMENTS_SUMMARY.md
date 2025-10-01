# V3-Alpha Improvements Summary

## Date: October 1, 2025

## Overview
This document summarizes the improvements made to the Grambo v3-alpha pipeline (grap3 -> graf -> grav) to achieve feature parity with v2-alpha and add enhanced capabilities.

## Issues Addressed

### 1. View Filtering by Timestamp ✅
**Problem**: The grav UI was showing ALL views in the view card, not just the view active at the current frame timestamp.

**Root Cause**: graf was storing views in a simple key-value state machine that only kept the latest view, but when retrieving views for frames, it was including all historical views without temporal filtering.

**Solution**: 
- Modified graf to store views in timestamped history lists (`wsrep_view_history` and `gcomm_view_history`)
- Updated frame builder to select only the most recent view at or before each frame's timestamp
- Views are now correctly filtered temporally

**Verification**:
```
✓ Frame 100 views show timestamps ≤ 2025-09-23T10:34:36
✓ Frame 200 views show timestamps ≤ 2025-09-25T17:22:20  
✓ Frame 400 views show timestamps ≤ 2025-09-27T10:40:25
```

### 2. Node State Transitions (from_state and to_state) ✅
**Problem**: Need to show both from_state and to_state in node state transitions for better state machine tracking.

**Status**: 
- ✅ grap3 already extracts both from_state and to_state from log patterns
- ✅ graf propagates both fields through frames
- ✅ graf sets node_state = to_state for backward compatibility
- ✅ grav visualization uses to_state (with node_state fallback) for colors and SST arrows
- ✅ Template displays both from_state and to_state in node details card

**Verification**:
```
Frame 10: NODE_11407
  from_state: SYNCED -> to_state: CLOSED
  node_state: CLOSED (equals to_state: ✓)

Frame 14: NODE_11407
  from_state: CLOSED -> to_state: OPEN
  node_state: OPEN (equals to_state: ✓)

Total state transitions found: 190
```

### 3. wsrep_view Entity Support ✅
**Problem**: Need to distinguish between gcomm views and wsrep views, as they represent different protocol layers.

**Status**:
- ✅ grap3 parses wsrep_view multiline blocks with proper timestamp extraction
- ✅ wsrep_view includes all fields: group_uuid, view_seq, status, protocol_version, capabilities, final, own_index, member_details
- ✅ graf handles wsrep_view as a separate layer alongside gcomm views
- ✅ grav displays both layers in the view card

**Fields**:
- `group_uuid`: The cluster UUID (same as cluster_uuid)
- `view_id`: Composite of group_uuid:view_seq
- `status`: PRIMARY/NON-PRIMARY
- `view_status`: PRIM/NON_PRIM (normalized)
- `protocol_version`: Integer (typically 4)
- `capabilities`: Array of capability strings
- `final`: Boolean
- `own_index`: Integer index of local node in member list
- `member_count`: Integer count of members
- `member_details`: Array of {index, uuid, node_name}
- `members`, `member_names`, `member_uuids`: Derived lists

**Verification**:
```
Total wsrep_view event frames: 150
✓ wsrep_view entities properly extracted with timestamps
✓ group_uuid field correctly used as cluster identifier
```

## Architecture Details

### Node Entity Design (Physical Nodes)
- **Unique by**: node_name (e.g., NODE_11407, NODE_21407, NODE_31407)
- **UUID tracking**: All UUIDs acquired by the node over its history
- **Fields**:
  - `node_name`: Physical node identifier
  - `long_uuid`: Current/latest full UUID
  - `uuid_history`: List of all UUIDs (full + short forms)
  - `cluster_uuid`: Cluster membership
  
**Example** (cl407 dataset has 3 physical nodes):
```json
{
  "node_name": "NODE_11407",
  "long_uuid": "3a42f33d-97f3-11f0-ae74-8ea804e8387d",
  "uuid_history": [
    "7a30da88-97e4-11f0-aef8-7e66bbcd8637",
    "7a30da88-aef8",
    "7a30da88-97e4-11f0-aef9-7e66bbcd8637",
    "7a30da88-aef9",
    "89c65b64-97f2-11f0-87c3-22481ca21bac",
    "89c65b64-87c3",
    "3a42f33d-97f3-11f0-ae74-8ea804e8387d",
    "3a42f33d-ae74"
  ]
}
```

### View Layers
The system tracks two view layers:

1. **gcomm view** (GCS layer):
   - View ID format: `uuid,seqno`
   - Contains: members, joined, left, partitioned lists
   - Represents low-level group communication state

2. **wsrep_view** (WSREP layer):
   - View ID format: `group_uuid:seqno`
   - Contains: members with full details, capabilities, protocol version
   - Represents high-level replication state

Both layers are tracked independently and can have different timestamps/sequences.

### Frame Generation Strategy
graf builds one frame per event, where each frame represents a complete cluster snapshot after applying that event:

1. **Temporal entities** create new frames (node_state, view, wsrep_view, sst, ist, error)
2. **Core entities** (node, cluster) provide baseline state
3. **State machine** tracks latest values for each property
4. **Frame timestamp** determines which historical values are active

This allows grav to show the exact cluster state at any point in time.

## Pipeline Flow

```
┌──────────┐      ┌──────┐      ┌──────┐
│  grap3   │ ───> │ graf │ ───> │ grav │
│  v3.0.0  │      │      │      │      │
└──────────┘      └──────┘      └──────┘
    │                 │              │
    │ JSON entities   │ JSON frames  │ Web UI
    │                 │              │
    ▼                 ▼              ▼
• node              • timestamp     • Timeline
• node_state        • index         • Node graph
• view (gcomm)      • event         • State cards
• wsrep_view        • nodes         • View details
• quorum            • clusters      • Quorum info
• sst/ist           • views         • Error log
• error             • quorum        • Log viewer
```

## Test Results

### Complete Pipeline Test
```bash
./grap3 cl407/error.*.log --format=json | ./graf > graffed3.json
```

**Results**:
- ✅ 837 frames generated
- ✅ All view timestamps correctly filtered (≤ frame timestamp)
- ✅ 190 state transitions with from_state/to_state
- ✅ 150 wsrep_view entities properly integrated
- ✅ Node UUID tracking across restarts working
- ✅ Temporal entity sequencing correct

### View Filtering Verification
All tested frames show proper temporal filtering:
- No future views appear in frames
- Most recent view at or before frame timestamp is selected
- Multiple clusters tracked independently
- Both gcomm and wsrep layers filtered correctly

### State Transition Verification
- from_state captures previous state
- to_state captures new state
- node_state mirrors to_state for compatibility
- Transitions tracked: SYNCED→CLOSED→OPEN→JOINED→SYNCED

## Files Modified

### graf (Frame Builder)
**File**: `graf`
**Changes**:
- Line 555-589: Changed view storage from single state to timestamped history lists
- Line 712-774: Updated frame builder to select views by timestamp
- Added `wsrep_view_history` and `gcomm_view_history` arrays
- Views now filtered temporally when building frames

**Impact**: 
- Fixes view card showing all views instead of current view
- Enables proper temporal view tracking
- Maintains backward compatibility

### grav (Visualization Server)
**File**: `grav`
**Status**: Already compatible
- Uses `to_state` || `node_state` for visualization (lines 48, 78, 903, 908, 947)
- Properly handles both v2 and v3 data formats
- No changes needed

### grap3 (Entity Extractor)
**File**: `grap3`
**Status**: Feature complete
- Extracts wsrep_view multiline blocks (lines 465-727)
- Proper timestamp extraction with hour zero-padding
- All wsrep_view fields captured
- No changes needed

## Comparison: v2 vs v3

### Common Features ✅
- ✅ Node tracking with UUID history
- ✅ View entities (both gcomm and wsrep)
- ✅ State transitions
- ✅ SST/IST tracking
- ✅ Error/warning classification
- ✅ Quorum events
- ✅ Temporal entity sequencing
- ✅ Frame-based state machine

### V3 Improvements ✅
- ✅ Cleaner entity type separation (wsrep_view vs view)
- ✅ Explicit from_state and to_state fields
- ✅ Proper timestamp-based view filtering
- ✅ Pattern-based extraction (maintainable)
- ✅ Better multiline parsing
- ✅ Consistent field naming

### Field Mappings

#### Node State
```
v2: node_state          v3: from_state, to_state, node_state
                            (node_state = to_state)
```

#### Views
```
v2: view (both types)   v3: view (gcomm), wsrep_view (wsrep)
    view_layer: wsrep       entity_type: wsrep_view
    cluster_uuid            group_uuid, cluster_uuid
```

## Known Limitations

1. **Timestamp Precision**: 1-second granularity (Galera log limitation)
2. **Multifile Ordering**: Events in same second across files cannot be ordered
3. **Node UUID Changes**: Requires full UUID history for accurate node tracking
4. **View Multiplicity**: Multiple view layers means multiple views per timestamp

## Next Steps

### Potential Enhancements
1. Add IST progress tracking visualization
2. Enhance error correlation with events
3. Add cluster topology graph
4. Implement diff view between frames
5. Add bookmark/annotation system
6. Export timeline as report

### Testing Recommendations
1. Test with larger datasets (>1000 frames)
2. Test with more complex split-brain scenarios
3. Test with rapid view changes
4. Verify memory usage with large histories
5. Test cross-version compatibility (v2 vs v3 graf frames)

## Conclusion

The v3-alpha pipeline now has full feature parity with v2-alpha and includes improved:
- ✅ Temporal view filtering
- ✅ State transition tracking with from_state/to_state
- ✅ wsrep_view entity support
- ✅ Node UUID history tracking
- ✅ Backward compatible with existing visualizations

All changes maintain the existing architecture while fixing critical bugs and adding necessary features for accurate Galera cluster analysis.
