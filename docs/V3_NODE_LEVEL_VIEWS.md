# V3 Node-Level Views Implementation

## Overview

This update transforms wsrep_view entities from cluster-level to node-level, properly reflecting that each node has its own view of the cluster at any given time.

## Key Changes

### 1. GRAP3 - Entity Extraction

**Wsrep View Enhancement:**
- Added `node_name` and `node_uuid` to each wsrep_view entity
- Node ownership determined from `own_index` field (member_details[own_index])
- Added `context: LOCAL` to indicate node-level view
- Renamed `view_uuid` to `group_uuid` to clarify it represents the cluster/group UUID
- `view_id` now properly combines `group_uuid:view_seq`

**Example wsrep_view entity:**
```json
{
  "entity_type": "wsrep_view",
  "entity_id": "wsrep_view_114",
  "timestamp": "2025-09-22 20:42:11",
  "context": "LOCAL",
  "node_name": "NODE_11407",
  "node_uuid": "d9c6d6f5-97e3-11f0-abb6-a63756ae0ed5",
  "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
  "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
  "view_seq": 1,
  "status": "PRIMARY",
  "own_index": 0,
  "member_count": 1,
  "members": ["NODE_11407"]
}
```

### 2. GRAF - Frame Building

**View Processing:**
- **Removed** cluster-level view aggregation (old approach)
- **Added** `node_wsrep_view` state machine for per-node view tracking
- Each node maintains its own view history
- Frames now include `node_views` field with per-node views
- **Skipped** gcomm views entirely (they use node UUIDs as view_id, not meaningful)

**Frame Structure:**
```json
{
  "index": 42,
  "timestamp": "2025-09-22 20:42:11",
  "nodes": { "NODE_11407": {...} },
  "clusters": { "cluster_d9c70dcb": {...} },
  "views": {},  // Legacy, kept for backward compatibility (empty)
  "node_views": {
    "NODE_11407": {
      "view_seq": 1,
      "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
      "member_count": 1,
      "node_name": "NODE_11407",
      ...
    }
  },
  "quorum": {...}
}
```

### 3. GRAV - Visualization

**Backend Updates:**
- `/frame/<idx>` endpoint now includes `node_view:` entities
- Format: `node_view:NODE_11407` → node's view data
- Legacy `view:` entities kept for backward compatibility

**UI Updates:**
- Views section now displays per-node wsrep views
- Groups views by cluster (group_uuid short form)
- Sorts views by timestamp descending (most recent first)
- Shows cluster header with view change timestamp
- Displays each node's view indented under its cluster
- Node cards properly show `from_state` and `to_state` (already supported)

**View Display Example:**
```
┌─ WSREP Views ──────────────────────────────────
│ cluster_d9c70dcb (view changed: 2025-09-22 20:42:11)
│   │ Node: NODE_11407
│   │   group_uuid: d9c70dcb-97e3-11f0-b2ad-4f637476a656
│   │   view_id: d9c70dcb-97e3-11f0-b2ad-4f637476a656:1
│   │   view_seq: 1
│   │   status: PRIMARY
│   │   member_count: 1
│   │   own_index: 0
│   │   members: NODE_11407
│   │
│   │ Node: NODE_21407
│   │   group_uuid: d9c70dcb-97e3-11f0-b2ad-4f637476a656
│   │   view_id: d9c70dcb-97e3-11f0-b2ad-4f637476a656:1
│   │   view_seq: 1
│   │   ...
└────────────────────────────────────────────────
```

## Architectural Rationale

### Why Node-Level Views?

1. **Reality of Galera**: Each node independently observes and reports cluster views
2. **Partition Detection**: During network partitions, different nodes see different views
3. **Debugging**: Essential to understand what each node perceived at each moment
4. **Temporal Correctness**: Views change at different timestamps for different nodes

### Removed gcomm Views

gcomm views were excluded because:
- They use node UUIDs (not cluster UUIDs) as `view_id`
- They don't provide additional information beyond wsrep views
- They created confusion in visualization
- wsrep views are more complete and accurate

### Node Ownership via own_index

The `own_index` field in wsrep view output indicates which member in the `members` list
represents the observing node. This is used to:
- Identify which node generated each view
- Link views to physical nodes (by matching against node UUID history)
- Enable proper per-node view tracking

## Frame Generation Logic

For each frame (event snapshot):
1. Find the most recent wsrep view for each node at or before frame timestamp
2. Include all node views active at that timestamp
3. Multiple nodes may have different views at the same timestamp (during partitions)
4. View sequence numbers are preserved but may not be monotonic across nodes

## Compatibility

- **Backward Compatible**: Old `views` field kept (empty) for legacy tools
- **V2 Support**: Grav propagates `node_state` → `to_state` for v2 frames
- **V3 Native**: Fully supports `from_state` and `to_state` in nodes
- **Mixed Mode**: Can visualize both v2 and v3 frame formats

## Testing

Test with cl407 logs:
```bash
./grap3 cl407/error.*.log --format=json > grap_out.json
./graf grap_out.json --ndjson > frames.ndjson
./grav --frames=frames.ndjson --logs cl407/*.log
```

Expected results:
- 3 physical nodes (NODE_11407, NODE_21407, NODE_31407)
- Each node has its own view history
- Views properly grouped by cluster UUID
- Most recent views shown first
- Node state transitions show both from and to states

## Next Steps

Potential enhancements:
1. Add view comparison visualization (show divergence during partitions)
2. Highlight view conflicts between nodes
3. Add view sequence timeline
4. Show view transition arrows between nodes
5. Detect and flag split-brain scenarios from view data

## Files Changed

- `grap3`: Added node ownership to wsrep_view entities
- `graf`: Implemented node-level view tracking and frame building
- `grav`: Added node_views support to frame endpoint
- `templates/index.html`: Updated UI to display node-level views
