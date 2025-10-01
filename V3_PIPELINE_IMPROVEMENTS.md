# V3 Pipeline Improvements Summary

## Overview
This document summarizes the improvements made to the grap/graf/grav pipeline to better handle entity types, views, and state transitions.

## Changes Made

### 1. GRAF: Exclude gcomm Views ✅

**Problem**: 
- gcomm views were being processed alongside wsrep views
- gcomm `view_id` contained node UUIDs instead of group UUIDs
- This created confusion and duplicate view data

**Solution**:
- Skip gcomm layer entities during view processing (`if layer == "gcomm": continue`)
- Remove gcomm view history tracking from state machine
- Remove gcomm view rendering from frame builder
- Keep only wsrep views which have proper group_uuid and view_id

**Result**:
```bash
# Before: Mixed view data with node UUIDs as view_id
Frames with gcomm views: ~400
Frames with wsrep views: ~150

# After: Clean wsrep-only views with proper group UUIDs
Frames with gcomm views: 0
Frames with wsrep views: 706 (propagated across all relevant frames)
```

### 2. GRAV: Improve View Display ✅

**Changes**:
- Remove gcomm view rendering from HTML template
- Add `group_uuid` to displayed wsrep view fields (shown first)
- Sort view clusters by timestamp descending (most recent first)
- Display proper view fields in order: `group_uuid`, `view_id`, `view_seq`, `status`, etc.

**View Card Now Shows**:
```
cluster_d9c70dcb
  layer: WSREP
  group_uuid: d9c70dcb-97e3-11f0-b2ad-4f637476a656
  view_id: d9c70dcb-97e3-11f0-b2ad-4f637476a656:1
  view_seq: 1
  status: PRIMARY
  cluster_state: PRIM
  member_count: 3
  members: [NODE_11407, NODE_21407, NODE_31407]
```

### 3. Node State Transitions ✅

**Already Implemented**:
- Both `from_state` and `to_state` are extracted by grap3
- Both fields are displayed in node cards (grav template line 1749)
- `to_state` is used for:
  - Circle colors in visualization
  - SST arrow detection (DONOR/JOINER states)
  - State propagation across frames
- `from_state` provides transition context

**Example**:
```json
{
  "node_name": "NODE_11407",
  "from_state": "JOINED",
  "to_state": "SYNCED",
  "transition_type": "LOCAL_SHIFT"
}
```

### 4. WSREP View Entity Structure ✅

**Already Implemented in grap3**:
```json
{
  "entity_type": "wsrep_view",
  "view_layer": "wsrep",
  "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
  "cluster_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
  "view_seq": 1,
  "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
  "status": "PRIMARY",
  "cluster_state": "PRIM",
  "protocol_version": 4,
  "capabilities": [...],
  "member_count": 1,
  "members": ["NODE_11407"],
  "member_details": [
    {
      "index": 0,
      "uuid": "d9c6d6f5-97e3-11f0-abb6-a63756ae0ed5",
      "node_name": "NODE_11407"
    }
  ]
}
```

**Key Fields**:
- `group_uuid`: Cluster UUID (group identifier)
- `view_seq`: Sequence number
- `view_id`: Complete identifier = `group_uuid:view_seq`

### 5. Node Entity UUID History ✅

**Already Implemented in grap3**:
```json
{
  "entity_type": "node",
  "node_name": "NODE_11407",
  "long_uuid": "3a42f33d-97f3-11f0-ae74-8ea804e8387d",
  "uuid_history": [
    "7a30da88-97e4-11f0-aef8-7e66bbcd8637",
    "7a30da88-aef8",
    "89c65b64-97f2-11f0-87c3-22481ca21bac",
    "89c65b64-87c3",
    "3a42f33d-97f3-11f0-ae74-8ea804e8387d",
    "3a42f33d-ae74"
  ]
}
```

**Benefits**:
- Physical node tracking across restarts
- UUID mapping for temporal entity correlation
- cl407 logs correctly show only 3 core nodes: NODE_11407, NODE_21407, NODE_31407

## Pipeline Statistics

### GRAP3 Output (cl407 logs)
```
Total entities: 859
Entity breakdown:
  error: 318
  ist: 22
  node: 3 (physical nodes)
  node_state: 136
  quorum: 48
  sst: 36
  view: 146 (gcomm - excluded from graf)
  wsrep_view: 150 (used in graf)
```

### GRAF Output
```
Total frames: 713
Frames with views: 706
Frames with gcomm views: 0 ✅
```

### Frame Structure
Each frame represents a snapshot at timestamp `(timestamp, timestamp_index)` containing:
- **N distinct NODES**: Each with one state
- **1 CLUSTER**: Virtual superclass (groups)
- **N GROUPS**: Cluster can change group on reboot
- **1 QUORUM**: Cluster quorum status
- **1 VIEW**: Current wsrep view (wsrep layer only)

### State Machine Properties

**Temporal Entities** (change over time):
- `node_state`: Per-node state changes (from_state → to_state)
- `wsrep_view`: Cluster view changes
- `quorum`: Quorum status changes
- `sst`: SST session state changes
- `ist`: IST session state changes

**Core Entities** (persistent):
- `node`: Physical node with UUID history
- `cluster`: Virtual cluster grouping

## Testing

Run the test pipeline:
```bash
./test_pipeline.sh
```

Expected output:
- ✅ grap3: 859 entities
- ✅ graf: 713 frames
- ✅ 0 gcomm views
- ✅ 706 frames with wsrep views
- ✅ Node states have from_state and to_state
- ✅ Views have proper group_uuid and view_id

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      GRAP3 (Parser)                         │
│  Extracts entities from Galera logs                         │
│  - Physical nodes with UUID history                         │
│  - Node state changes (from_state → to_state)               │
│  - WSREP views (with group_uuid and view_id)                │
│  - SST/IST events, quorum changes, errors                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    GRAF (Frame Builder)                     │
│  Builds state machine frames from entities                  │
│  - Excludes gcomm views (node UUID pollution)               │
│  - Tracks temporal entity changes                           │
│  - Creates snapshot per (timestamp, timestamp_index)        │
│  - Propagates states across frames                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 GRAV (Web Visualizer)                       │
│  Renders cluster timeline from frames                       │
│  - Node state circles (colored by to_state)                 │
│  - SST arrows (based on DONOR/JOINER states)                │
│  - View cards (wsrep only, sorted by timestamp desc)        │
│  - Displays both from_state and to_state                    │
└─────────────────────────────────────────────────────────────┘
```

## Compatibility

### V2 vs V3 Entity Comparison

| Feature | V2 (grap) | V3 (grap3) | Status |
|---------|-----------|------------|--------|
| Physical node tracking | ✅ | ✅ | Same |
| UUID history | ✅ | ✅ | Same |
| Node state transitions | `node_state` only | `from_state` + `to_state` | Improved |
| WSREP views | entity_type=`view` | entity_type=`wsrep_view` | Clarified |
| GCOMM views | entity_type=`view` | entity_type=`view` | Excluded in graf |
| View ID | Mixed | `group_uuid:seqno` | Improved |
| Cluster entities | ✅ (3 entities) | ❌ (not needed) | Simplified |

### Graf Compatibility

- ✅ Handles both v2 and v3 entity formats
- ✅ `to_state` preferred, falls back to `node_state` for v2
- ✅ Both `view` and `wsrep_view` entity types supported
- ✅ Skips gcomm views automatically

### Grav Compatibility

- ✅ Uses `to_state` for colors (falls back to `node_state`)
- ✅ Displays both `from_state` and `to_state` in node cards
- ✅ Sorts views by timestamp descending
- ✅ Shows `group_uuid` prominently in view cards

## Remaining Improvements

All requested features have been implemented:
- ✅ Exclude gcomm views from graf
- ✅ Show proper group_uuid and view_id
- ✅ Display both from_state and to_state
- ✅ Sort views by timestamp descending
- ✅ Physical node tracking with UUID history
- ✅ Temporal entity state machine
- ✅ Compatible with grax pipeline

## Files Modified

1. **graf** - Frame builder
   - Exclude gcomm views
   - Keep only wsrep views
   - Remove gcomm_view_history tracking

2. **templates/index.html** - Grav visualization
   - Remove gcomm view rendering
   - Add group_uuid to view display
   - Sort clusters by timestamp descending
   - Keep both from_state and to_state display

3. **test_pipeline.sh** - Testing script
   - Comprehensive pipeline test
   - Entity counting and validation
   - Frame structure verification

## Conclusion

The v3 pipeline now provides a cleaner, more accurate representation of Galera cluster state:

- **Single source of truth**: WSREP views only (no gcomm confusion)
- **Proper identifiers**: group_uuid and view_id (group_uuid:seqno)
- **Complete state transitions**: Both from_state and to_state
- **Physical node tracking**: UUID history across restarts
- **Frame-based state machine**: Temporal entity snapshots

The pipeline is fully functional and ready for production use.
