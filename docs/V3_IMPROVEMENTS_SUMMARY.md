# V3-Alpha Improvements Summary

## Overview
This document summarizes the improvements made to the v3-alpha branch based on comparison with v2-alpha and user requirements for the grap→graf→grav pipeline.

## Key Improvements

### 1. Enhanced Node State Pattern Coverage

#### Added Patterns:
- **Server Status Change Pattern**: Captures internal server status transitions
  - States: CONNECTED, DISCONNECTED, DISCONNECTING, INITIALIZED, INITIALIZING, DONOR
  - Example: `Server status change connected -> joiner`
  - Pattern ID: `server_status_change`
  - Confidence: 0.98

- **Synchronized with Group Pattern**: Captures final sync confirmation
  - Transition: JOINING → SYNCED
  - Example: `Synchronized with group, ready for connections`
  - Pattern ID: `synchronized_with_group`
  - Confidence: 0.99

#### State Normalization:
- All state names now normalized to uppercase for consistency
- Both `from_state` and `to_state` properly tracked
- Examples: CONNECTED, SYNCED, JOINER, DONOR, etc.

### 2. WSREP View Entity Improvements

#### Node Name Resolution:
- **Problem**: Some wsrep_view entities had `node_name: "unknown"` when:
  - View had no members (NON_PRIMARY state)
  - own_index was -1
  
- **Solution**: Added fallback to `self.local_node_name` and `self.local_node_uuid`
  - Result: **100% of wsrep_views now have node_name**
  - No more "unknown" nodes in visualization

#### Entity Type Separation:
- V2 mixed both gcomm and wsrep views as entity_type="view"
- V3 separates them:
  - entity_type="view" for gcomm views
  - entity_type="wsrep_view" for wsrep protocol views
  
#### Graf Integration:
- Graf already excludes gcomm views (line 563-564)
- Properly handles wsrep_view entities
- Tracks per-node view history

### 3. GRAV UI Enhancements

#### View Card Improvements:
- **Hidden Fields**: Removed `view_id` (redundant with `group_uuid:view_seq`)
- **Capabilities Display**: 
  - Shows first 3 capabilities, truncated with "..."
  - Full list available on hover (tooltip)
  - Example: "MULTI-MASTER, CERTIFICATION, PARALLEL_APPLYING..."

#### View Ordering:
- Views sorted by timestamp DESC (most recent first)
- "view changed" timestamp displayed inline with cluster name

#### Node State Display:
- Both `from_state` and `to_state` displayed in node card
- Circle colors based on `to_state` (as required)
- SST arrows work correctly with `to_state`

## Entity Count Comparison

### V2 vs V3 Entity Counts:

| Entity Type  | V2    | V3    | Difference | Notes                        |
|--------------|-------|-------|------------|------------------------------|
| cluster      | 3     | 0     | -3         | Implicit in V3               |
| error        | 505   | 318   | -187       | Different pattern coverage   |
| ist          | 6     | 22    | +16        | More in V3                   |
| node         | 3     | 3     | 0          | Same                         |
| node_state   | 390   | 345   | -45        | Close, different patterns    |
| quorum       | 73    | 48    | -25        | Different pattern coverage   |
| sst          | 66    | 36    | -30        | Different pattern coverage   |
| view         | 428   | 146   | -282       | Gcomm views (excluded in graf)|
| wsrep_view   | 0     | 150   | +150       | NEW in V3                    |
| **TOTAL**    | **1474** | **1068** | **-406**  |                            |

### V3 Node State Pattern Breakdown:

| Pattern Name             | Count |
|--------------------------|-------|
| node_state_restored      | 18    |
| node_state_shifting      | 118   |
| server_status_change     | 173   |
| synchronized_with_group  | 36    |
| **TOTAL**                | **345**|

## Architecture Notes

### Node Tracking (CORE Entity):
- Physical nodes identified by `node_name`: NODE_11407, NODE_21407, NODE_31407
- Each node maintains `uuid_history` list (both long and short forms)
- All temporal entities map to physical nodes via UUID resolution

### Temporal Entities:
Each frame should have:
- **N × NODES**: Each with one state (to_state)
- **1 × CLUSTER**: Virtual superclass
- **N × GROUPS**: One cluster can have multiple groups over time
- **1 × QUORUM**: Current quorum state
- **1 × VIEW per node**: Latest wsrep_view from each node's perspective

### Frame Building (Graf):
- Excludes gcomm views (less noise, redundant with wsrep_view)
- Builds per-node wsrep_view section
- Tracks latest state for each entity type per frame
- Supports both v2 (node_state) and v3 (from_state/to_state) formats

## Files Modified

1. **grap3** (Entity Extraction):
   - Added `server_status_change` pattern
   - Added `synchronized_with_group` pattern
   - Added state name normalization
   - Fixed wsrep_view node_name fallback

2. **templates/index.html** (Visualization):
   - Updated view card field list
   - Added capabilities truncation with tooltip
   - Already handles from_state/to_state correctly

3. **graf** (Frame Building):
   - Already working correctly
   - Excludes gcomm views
   - Handles wsrep_view entities

## Testing

### Test Commands:
```bash
# Run full pipeline
./grap3 cl407/error.*.log --format=json > grap_output.json
./graf grap_output.json --ndjson > graf_frames.ndjson
./grav --frames=graf_frames.ndjson --host=127.0.0.1 --port=5002 --logs cl407/*.log

# Or use grax wrapper
./grax cl407/error.*.log
```

### Validation:
- ✅ 150 wsrep_view entities generated
- ✅ 100% have node_name (no "unknown" nodes)
- ✅ 345 node_state entities with proper from_state/to_state
- ✅ All states normalized to uppercase
- ✅ Graf properly builds frames with per-node views
- ✅ GRAV displays views with truncated capabilities

## Future Work

Potential areas for further improvement:
1. Increase error entity coverage to match V2 (currently 318 vs 505)
2. Investigate quorum pattern differences (48 vs 73)
3. Consider adding more SST patterns (36 vs 66)
4. Document exact differences in pattern matching philosophy

## Conclusion

V3-alpha now provides:
- More accurate node state tracking with separate from/to states
- Cleaner view entity model (wsrep_view vs mixed view)
- Better UI presentation (truncated capabilities, hidden redundant fields)
- 100% node name coverage in wsrep_views
- Compatible with graf frame-based state machine
- Ready for timeline visualization in grav

The pipeline is fully functional: **grap3 → graf → grav**
