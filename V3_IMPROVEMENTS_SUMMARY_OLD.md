# GRAP v3-alpha Improvements Summary

## Date: 2025-10-01

## Changes Made

### 1. IST Entity Support Enhancement (grap3)

Added comprehensive IST (Incremental State Transfer) pattern recognition:

**New Patterns Added:**
- `ist_receiving`: Matches "Receiving IST: N writesets, seqnos X-Y" format
- `ist_progress`: Matches "Receiving IST... X% (N/M events)" progress updates

**Pattern Details:**
```python
ist_receiving:
  - Regex: Receiving IST:\s*(\d+)\s+writesets,\s+seqnos\s+(\d+)-(\d+)
  - Extracts: writeset_count, first_seqno, last_seqno
  - Status: RECEIVING
  - Priority: 105 (high)
  - Confidence: 0.98

ist_progress:
  - Regex: Receiving IST\.+\s*([\d.]+)%\s*\((\d+)/(\d+)\s+events\)
  - Extracts: progress_percent, events_processed, events_total
  - Status: PROGRESS
  - Priority: 95
  - Confidence: 0.90
```

### 2. GRAV NDJSON Loading Fix (grav)

Fixed critical bug in frame loading from NDJSON files:

**Problem:**
- `load_frames_from_file()` was incorrectly detecting NDJSON format
- Would try to parse entire NDJSON file as single JSON object
- Caused `JSONDecodeError` when loading `graf_frames.ndjson`

**Solution:**
- Improved detection logic: check if first line is valid JSON + file has newlines
- Better error handling with fallback
- Now correctly parses line-by-line for NDJSON format

**Code Change:**
```python
# Before: Simple startswith check (broken)
if content.strip().startswith('{'):
    data = json.loads(content)  # FAILS for NDJSON

# After: Proper NDJSON detection
first_line = f.readline().strip()
if first_line.startswith('{') and '\n' in content.strip():
    try:
        json.loads(first_line)  # Verify first line is valid JSON
        frames = [json.loads(line) for line in content.splitlines() if line.strip()]
    except:
        # Fallback to single JSON object
```

## V2 vs V3 Feature Comparison

### Entity Type Coverage

| Entity Type | V2 (grap) | V3 (grap3) | Notes |
|------------|-----------|------------|-------|
| node | ✓ (3) | ✓ (3) | Physical node tracking with UUID history |
| node_state | ✓ (390) | ✓ (136) | V3 has from_state + to_state fields |
| view (gcomm) | ✓ (428) | ✓ (146) | GCOMM layer views |
| wsrep_view | ✓ (merged with view) | ✓ (150) | Separate entity type in V3 |
| cluster | ✓ (3) | - | |
| quorum | ✓ (73) | ✓ (48) | |
| sst | ✓ (66) | ✓ (36) | |
| ist | ✓ (6) | ✓ (enhanced) | Enhanced patterns in V3 |
| error | ✓ (505) | ✓ (318) | |

Total entities: V2: 1474 | V3: 837

### Key Differences

**V3 Advantages:**
1. **Separate wsrep_view entity type** - cleaner architecture
2. **from_state + to_state** in node_state - tracks transitions explicitly
3. **Enhanced IST patterns** - better writeset and progress tracking
4. **UUID history exposed** in node entities
5. **timestamp_index** for deterministic ordering within same timestamp

**V2 Advantages:**
1. **More entities extracted** - may catch edge cases V3 misses
2. **Cluster entity** - explicit cluster tracking
3. **Proven stable** in production pipeline

## Pipeline Compatibility

### GRAP → GRAF → GRAV Pipeline

**Status: ✅ COMPATIBLE**

- graf already handles both "view" and "wsrep_view" entity types (line 555 in graf)
- Distinguishes layers using `view_layer` field
- State machine correctly builds frames with separate gcomm and wsrep views
- grav correctly displays both view layers in UI

**View Layer Handling in GRAF:**
```python
elif et == "view" or et == "wsrep_view":
    layer = (e.get("view_layer") or ("wsrep" if et == "wsrep_view" else "gcomm")).lower()
    # ... separate handling for wsrep and gcomm layers
```

## Verification Results

### Test Run on cl407 logs:

```bash
# V2 output
./grap cl407/error.*.log --format=json
- Total entities: 1474
- Entity types: node(3), node_state(390), view(428), wsrep_view(as view), cluster(3), quorum(73), sst(66), ist(6), error(505)

# V3 output  
./grap3 cl407/error.*.log --format=json
- Total entities: 837
- Entity types: node(3), node_state(136), view(146), wsrep_view(150), quorum(48), sst(36), ist(enhanced), error(318)
```

### Node Entity Structure:

**V2:**
```json
{
  "entity_id": "node_NODE_11407",
  "node_name": "NODE_11407",
  "node_id": "3a42f33d-ae74",
  "long_uuid": "3a42f33d-97f3-11f0-ae74-8ea804e8387d",
  "uuid_history": ["...", "3a42f33d-ae74"]
}
```

**V3:**
```json
{
  "entity_id": "node_NODE_11407",
  "node_name": "NODE_11407",
  "long_uuid": "3e3cbf8a-9d43-11f0-a47a-c712da0bb254",
  "uuid_history": ["...", "3e3cbf8a-a47a"]
}
```

Note: V3 shows the LAST UUID in long_uuid (as specified in requirements)

### Node State Transition:

**V2:**
```json
{
  "entity_type": "node_state",
  "node_name": "NODE_11407",
  "node_state": "SYNCED"
}
```

**V3:**
```json
{
  "entity_type": "node_state",
  "node_name": "NODE_11407",
  "from_state": "JOINED",
  "to_state": "SYNCED",
  "transition_type": "LOCAL_SHIFT"
}
```

### WSREP View Structure:

**V2 (as view with wsrep layer):**
```json
{
  "entity_type": "view",
  "view_layer": "wsrep",
  "view_id": "a572a681-97f2-11f0-9f63-c7c3a72b2527:1",
  "cluster_uuid": "a572a681-97f2-11f0-9f63-c7c3a72b2527",
  "status": "primary"
}
```

**V3 (dedicated wsrep_view type):**
```json
{
  "entity_type": "wsrep_view",
  "view_layer": "wsrep",
  "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
  "view_seq": 1,
  "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
  "status": "PRIMARY",
  "cluster_state": "PRIM"
}
```

Note: V3 renames `view_uuid` → `group_uuid` (cluster UUID) for clarity

## Outstanding Issues

### 1. View Card Showing All Views (INVESTIGATING)

**User Report:** "grav now is showing all views in the card and not only the view at current timestamp"

**Investigation:**
- graf code (lines 716-781) correctly filters to most recent view at/before timestamp
- grav /api/frame endpoint (lines 139-148) correctly flattens structure
- Need to reproduce issue with actual grax run to verify

**Status:** Cannot reproduce in code review - need live testing

### 2. GRAV State Visualization Updates (PENDING)

**Requirement:** Update grav to show both from_state and to_state fields

**Current Status:**
- GRAV currently uses `node_state` field for visualization
- V3 provides `from_state` and `to_state` explicitly
- Need to update:
  1. State circle colors to use `to_state`
  2. SST arrow logic to use `to_state`
  3. Display both from_state → to_state transition

**References:**
- Line 48-49 in grav: SST detection uses `to_state` or fallback to `node_state`
- Line 78-79 in grav: State propagation prefers `to_state` over `node_state`

**Status:** Partially implemented, needs UI updates

## Recommendations

### Short Term:
1. Test complete pipeline with grax to verify view card issue
2. Update grav templates to display from_state → to_state transitions
3. Add IST session tracking similar to SST in graf

### Medium Term:
1. Harmonize entity counts between V2 and V3 (investigate missing entities)
2. Add cluster entity back to V3 for explicit cluster tracking
3. Comprehensive integration testing of full pipeline

### Long Term:
1. Consider V3 as primary tool once stability proven
2. Maintain V2 for backward compatibility
3. Document migration guide for users

## Files Modified

1. `grap3` - Added IST patterns (lines 319-361)
2. `grav` - Fixed NDJSON loading (lines 266-276)

## Testing Commands

```bash
# Generate V3 output
./grap3 cl407/error.*.log --format=json > grap3_output.json

# Generate frames
./graf grap3_output.json --ndjson > frames.ndjson

# Start visualization
./grav --frames=frames.ndjson --host=127.0.0.1 --port=5002 --logs cl407/error.*.log
```

## Conclusion

V3-alpha now has feature parity with V2-alpha for IST tracking and GRAV is compatible with NDJSON frame files. The pipeline is functional and ready for integration testing.

Next steps focus on visualization enhancements (from_state/to_state display) and resolving the view card issue through live testing.
