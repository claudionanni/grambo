# View Card Fix - Field Name Mismatch Resolution

**Date**: October 1, 2025  
**Status**: ✅ FIXED

## Problem

The View Card in the grav web UI was not displaying/updating view information. The issue was traced to a field name mismatch between grap3 output and the HTML template expectations.

### Root Cause

**Field Name Inconsistency**:
- **grap2 (v2)**: Used `cluster_state` field for view status (e.g., "PRIM", "NON_PRIM")
- **grap3 (v3-alpha)**: Was using `view_status` field instead
- **HTML template**: Expected `cluster_state` field

This caused the template to not find the status information and fail to render view data.

## Solution

Updated three components to ensure consistency:

### 1. **grap3** - Changed field name from `view_status` to `cluster_state`
- Lines 163, 513, 564, 644
- Affects both wsrep_view and gcomm view entities
- Now matches v2 format exactly

### 2. **graf** - Added gcomm view layer to frames
- Previously only extracted wsrep_view layer
- Now extracts both wsrep and gcomm views
- Each layer stored with `view_layer` field

### 3. **grav** - Fixed JSON format loading
- Fixed logic for detecting JSON vs JSONL format
- Now correctly extracts frames array from JSON object

### 4. **templates/index.html** - Updated view card rendering
- Changed from looking for single "best" view to displaying all view layers
- Groups views by cluster
- Displays both WSREP and GCOMM layers side-by-side
- Shows layer-specific fields:
  - WSREP: status, cluster_state, view_id, view_seq, member_count, own_index, members, capabilities
  - GCOMM: cluster_state, view_id, view_seq, member_count, members, joined, left, partitioned

## Test Results

### Field Consistency

**grap3 output**:
```json
{
  "entity_type": "wsrep_view",
  "view_layer": "wsrep",
  "cluster_state": "PRIM",     ← Fixed
  "status": "PRIMARY",
  "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
  "member_count": 1
}
```

**gcomm view output**:
```json
{
  "entity_type": "view",
  "view_layer": "gcomm",
  "cluster_state": "PRIM",     ← Fixed
  "view_id": "d9c6d6f5-abb6,1",
  "member_count": 1,
  "members": ["d9c6d6f5-abb6"],
  "joined": [],
  "left": [],
  "partitioned": []
}
```

### Frame Generation

**graf output** (frames with both layers):
```
Frames with wsrep view: 829 ✅
Frames with gcomm view: 833 ✅
Total frames: 837
```

### API Response

**grav /frame/10 endpoint**:
```json
{
  "entities": {
    "view:cluster_d9c70dcb:wsrep": {
      "view_layer": "wsrep",
      "cluster_state": "PRIM",     ← Fixed
      "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
      "member_count": 1
    },
    "view:view_68:gcomm": {
      "view_layer": "gcomm",
      "cluster_state": "PRIM",     ← Fixed
      "view_id": "d9c6d6f5-abb6,1",
      "member_count": 1
    }
  }
}
```

## UI Changes

### Before
- View card showed: "No view data for this frame"
- Missing cluster_state field prevented rendering

### After
- View card displays both WSREP and GCOMM layers
- Shows cluster name as subtitle
- Each layer has its own section with relevant fields
- Both layers update properly as timeline advances

### Visual Layout
```
┌─────────────────────────────────────┐
│ Views (WSREP + GCOMM)               │
├─────────────────────────────────────┤
│ cluster_d9c70dcb                    │
│                                     │
│ layer: WSREP                        │
│ status: PRIMARY                     │
│ cluster_state: PRIM                 │
│ view_id: d9c70dcb...:1             │
│ member_count: 1                     │
│ members: [NODE_11407]               │
│                                     │
│ layer: GCOMM                        │
│ cluster_state: PRIM                 │
│ view_id: d9c6d6f5-abb6,1           │
│ member_count: 1                     │
│ members: [d9c6d6f5-abb6]           │
│ joined: []                          │
│ left: []                            │
│ partitioned: []                     │
└─────────────────────────────────────┘
```

## Files Changed

1. **grap3** (+4, -4): Changed `view_status` to `cluster_state`
2. **graf** (+11): Added gcomm view extraction to frames
3. **grav** (+5, -2): Fixed JSON format detection
4. **templates/index.html** (+52, -29): Updated view card rendering logic

## Compatibility

✅ **Backward Compatible**: 
- V2 format already used `cluster_state`
- V3 now matches V2 exactly
- No breaking changes to existing data

✅ **Forward Compatible**:
- Template handles both wsrep and gcomm layers
- Gracefully handles missing layers
- Works with any number of clusters

## Related

- **Original issue**: View card not updating in grav UI
- **Comparison files**: grap2_wsrep_view vs grap3_wsrep_view
- **Pipeline**: grap3 → graf → grav (fully working)

---

**Commit**: [next commit]  
**Branch**: v3-alpha  
**Status**: ✅ PRODUCTION READY
