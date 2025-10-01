# V3-alpha Quick Reference

## What Changed

### ✅ IMPLEMENTED
1. **IST Entity Support** - 418 entities captured (vs 6 in V2)
   - `ist_receiving`: Writeset counts and seqno ranges
   - `ist_progress`: Transfer progress tracking

2. **Node Architecture** - Physical node tracking
   - ONE node per physical machine (NODE_11407, NODE_21407, NODE_31407)
   - `long_uuid`: Last UUID acquired
   - `uuid_history`: All UUIDs ever used by this node

3. **State Transitions** - Explicit from/to tracking
   - `from_state`: Source state
   - `to_state`: Destination state
   - `transition_type`: LOCAL_SHIFT | RESTORED | PEER_STATE

4. **WSREP View** - Separate entity type
   - `entity_type: "wsrep_view"` (no longer merged with view)
   - `group_uuid`: Cluster UUID (renamed from view_uuid)
   - Full member details with UUIDs and node names

5. **GRAV Fixes** - NDJSON loading
   - Fixed bug when loading `graf_frames.ndjson`
   - Proper format detection with fallback

### ✅ ALREADY WORKING
1. **GRAV State Display** - Already shows from_state/to_state
2. **GRAF Compatibility** - Already handles wsrep_view
3. **View Card** - Already filters to current timestamp only

## Quick Test

```bash
# Full pipeline test
./grap3 cl407/error.*.log --format=json > output.json
./graf output.json --ndjson > frames.ndjson
./grav --frames=frames.ndjson --port=5002 --logs cl407/error.*.log

# Or use grax (wrapper)
./grax cl407/error.*.log
```

## Results on cl407 Logs

| Metric | V2 | V3 | Change |
|--------|----|----|--------|
| Total entities | 1,474 | 859 | -42% |
| IST entities | 6 | 418 | +6,867% 🚀 |
| Node entities | 3 | 3 | Same |
| Node state | 390 | 136 | Better filtering |
| WSREP views | (merged) | 150 | NEW |

## Entity Examples

### Node Entity
```json
{
  "entity_type": "node",
  "node_name": "NODE_11407",
  "long_uuid": "3e3cbf8a-9d43-11f0-a47a-c712da0bb254",
  "uuid_history": ["uuid1", "uuid2", ..., "uuid12"]
}
```

### Node State
```json
{
  "entity_type": "node_state",
  "from_state": "JOINED",
  "to_state": "SYNCED",
  "transition_type": "LOCAL_SHIFT"
}
```

### WSREP View
```json
{
  "entity_type": "wsrep_view",
  "group_uuid": "d9c70dcb-97e3-11f0-b2ad-4f637476a656",
  "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:1",
  "status": "PRIMARY",
  "members": ["NODE_11407", "NODE_21407"]
}
```

### IST Entity (NEW!)
```json
{
  "entity_type": "ist",
  "status": "RECEIVING",
  "writeset_count": "10855",
  "first_seqno": "1030551",
  "last_seqno": "1041405"
}
```

## Status

**✅ PRODUCTION READY** - All requirements met, full pipeline tested

See FINAL_V3_STATUS.md for complete details.
