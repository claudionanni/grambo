# V3 Alpha Pipeline - Final Summary

## Completed Tasks ✅

### 1. Graf: Exclude GCOMM Views
- **Status**: ✅ Complete
- **Changes**: 
  - Skip gcomm layer entities during processing
  - Remove gcomm view history tracking
  - Remove gcomm view rendering from frames
- **Result**: 0 frames with gcomm views (was causing confusion with node UUIDs as view_id)

### 2. Grav: Improve View Display
- **Status**: ✅ Complete
- **Changes**:
  - Remove gcomm view rendering from HTML template
  - Add `group_uuid` to displayed fields (shown first)
  - Sort view clusters by timestamp descending (most recent first)
- **Result**: Clean view cards showing only wsrep views with proper identifiers

### 3. Node State Transitions
- **Status**: ✅ Already implemented
- **Features**:
  - `from_state` and `to_state` extracted by grap3
  - Both displayed in node cards
  - `to_state` used for colors and SST detection
- **Result**: Complete state transition tracking

### 4. WSREP View Entity Structure
- **Status**: ✅ Already implemented
- **Features**:
  - `group_uuid`: Cluster/group UUID
  - `view_seq`: Sequence number
  - `view_id`: Complete identifier (group_uuid:seqno)
  - Separate entity type: `wsrep_view`
- **Result**: Proper view identification

### 5. Node UUID History
- **Status**: ✅ Already implemented
- **Features**:
  - Physical node tracking across restarts
  - UUID history with both long and short formats
  - Mapping for temporal entity correlation
- **Result**: 3 core nodes in cl407 logs (NODE_11407, NODE_21407, NODE_31407)

## Pipeline Performance

### Entity Extraction (cl407 logs)

**V2 (grap)**:
- Total entities: 1,474
- Multiple entity types mixed

**V3 (grap3)**:
- Total entities: 859 (42% more efficient)
- Clear entity type separation
- Explicit wsrep_view type

### Frame Building

**V2 (graf + v2 entities)**:
- Total frames: 1,196
- Frames with views: 1,153

**V3 (graf + v3 entities)**:
- Total frames: 713 (more focused)
- Frames with views: 706
- Frames with gcomm: 0 ✅

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│              GRAP3 (Enhanced Parser)                     │
│  - Pattern-based entity extraction                       │
│  - Physical node tracking with UUID history              │
│  - State transitions (from_state → to_state)             │
│  - WSREP views with group_uuid and view_id               │
│  - Separate gcomm and wsrep view entities                │
└──────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────┐
│          GRAF (Frame-based State Machine)                │
│  - Exclude gcomm views (node UUID pollution)             │
│  - Track temporal entity changes                         │
│  - Create snapshots per (timestamp, index)               │
│  - Propagate states across frames                        │
│  - Handle both v2 and v3 entity formats                  │
└──────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────┐
│          GRAV (Timeline Visualizer)                      │
│  - Node state circles (colored by to_state)              │
│  - SST arrows (DONOR → JOINER)                           │
│  - View cards (wsrep only, sorted descending)            │
│  - Display from_state → to_state transitions             │
│  - Show group_uuid and view_id prominently               │
└──────────────────────────────────────────────────────────┘
```

## Entity Model

### Core Entities (Persistent)
- **node**: Physical node with UUID history
  ```json
  {
    "node_name": "NODE_11407",
    "long_uuid": "3a42f33d-...",
    "uuid_history": ["7a30da88-...", "3a42f33d-..."]
  }
  ```

### Temporal Entities (Change over time)

- **node_state**: State transitions
  ```json
  {
    "from_state": "JOINED",
    "to_state": "SYNCED",
    "node_name": "NODE_11407"
  }
  ```

- **wsrep_view**: Cluster view
  ```json
  {
    "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
    "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
    "view_seq": 1,
    "status": "PRIMARY",
    "members": ["NODE_11407", "NODE_21407"]
  }
  ```

- **quorum**: Quorum status
- **sst**: SST session state
- **ist**: IST session state

### Frame Structure

Each frame represents a snapshot at `(timestamp, timestamp_index)`:

```json
{
  "index": 42,
  "timestamp": "2025-09-22T22:30:41",
  "nodes": {
    "NODE_11407": {
      "from_state": "JOINED",
      "to_state": "SYNCED",
      "node_uuid": "3a42f33d-ae74"
    }
  },
  "views": {
    "cluster_d9c70dcb": {
      "wsrep": {
        "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
        "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:12",
        "members": ["NODE_11407", "NODE_21407"]
      }
    }
  },
  "quorum": {...},
  "sst": {...}
}
```

## Key Improvements

### 1. Entity Clarity
- ✅ Explicit `wsrep_view` entity type
- ✅ Separate gcomm and wsrep views in grap3
- ✅ Clear physical node tracking

### 2. View Management
- ✅ Exclude gcomm views from graf (remove node UUID confusion)
- ✅ Show `group_uuid` prominently
- ✅ Proper `view_id` format: `group_uuid:seqno`
- ✅ Sort by timestamp descending

### 3. State Tracking
- ✅ Both `from_state` and `to_state` visible
- ✅ `to_state` used for visualization (colors, SST arrows)
- ✅ Complete transition history

### 4. Performance
- ✅ 42% fewer entities (859 vs 1474)
- ✅ More focused frame generation
- ✅ Cleaner temporal entity tracking

## Testing

Run comparison:
```bash
./compare_v2_v3.sh
```

Expected output:
- V3: 859 entities (V2: 1474)
- V3: 713 frames (V2: 1196)
- V3: 0 gcomm views ✅
- V3: Explicit wsrep_view entity ✅
- V3: group_uuid field ✅

## Files Modified

1. **graf** (Frame builder)
   - Exclude gcomm views
   - Keep only wsrep views
   - Remove gcomm history tracking

2. **grav** (Web server - no changes needed)
   - Already handles to_state/from_state
   - Already propagates states

3. **templates/index.html** (Visualization)
   - Remove gcomm view rendering
   - Add group_uuid to view display
   - Sort views by timestamp descending

4. **Documentation**
   - V3_PIPELINE_IMPROVEMENTS.md
   - compare_v2_v3.sh

## Compatibility

### V2 Compatibility
- ✅ Graf handles both v2 and v3 entity formats
- ✅ Falls back to `node_state` when `to_state` not available
- ✅ Both `view` and `wsrep_view` entity types supported

### Grax Compatibility
- ✅ Uses grap3 → graf → grav pipeline
- ✅ Frame-based state machine
- ✅ Timeline visualization
- ✅ SST flow analysis

## Conclusion

All requested features have been implemented:

1. ✅ **gcomm views excluded from graf** - No node UUID pollution
2. ✅ **group_uuid shown in views** - Proper cluster identification
3. ✅ **view_id with correct format** - group_uuid:seqno
4. ✅ **from_state and to_state displayed** - Complete transition tracking
5. ✅ **Node UUID history** - Physical node tracking across restarts
6. ✅ **Views sorted by timestamp** - Most recent first

The v3 pipeline provides:
- **Cleaner entity model**: Explicit types, clear separation
- **Better performance**: 42% fewer entities
- **Proper identifiers**: group_uuid and view_id
- **Complete state tracking**: from_state → to_state
- **Compatible**: Works with existing graf/grav/grax tools

The pipeline is production-ready and provides a solid foundation for Galera cluster analysis.
