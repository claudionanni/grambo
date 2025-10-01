# V3-Alpha Quick Reference

## Quick Start

```bash
# Run the complete pipeline
./grap3 cl407/error.*.log --format=json | ./graf --ndjson > frames.ndjson

# Verify everything works
./verify_v3_complete.sh

# Compare v2 vs v3
./compare_v2_v3.sh
```

## Key Changes Summary

### 1. GRAF: No More GCOMM Views ✅
**Before**: Mixed gcomm and wsrep views causing confusion
**After**: Pure wsrep views with proper group_uuid and view_id

### 2. GRAV: Better View Display ✅
- group_uuid shown first
- view_id with proper format (group_uuid:seqno)
- Sorted by timestamp descending (most recent first)
- No gcomm layer clutter

### 3. Node States: Complete Transitions ✅
**Before**: Only `node_state`
**After**: Full transition tracking with `from_state` → `to_state`

## Performance Improvements

| Metric | V2 | V3 | Improvement |
|--------|----|----|-------------|
| Entities | 1,474 | 859 | **42% fewer** ⚡ |
| Frames | 1,196 | 713 | **40% fewer** 🎯 |
| View Clarity | Mixed | Pure wsrep | **Much clearer** ✨ |
| State Tracking | node_state | from→to | **Complete** 📊 |

## Verification

```bash
./verify_v3_complete.sh
```

Expected: **✅ ALL CHECKS PASSED**

## Documentation

- `FINAL_V3_SUMMARY.md` - Executive summary
- `V3_PIPELINE_IMPROVEMENTS.md` - Comprehensive guide
- `compare_v2_v3.sh` - Comparison tool
- `verify_v3_complete.sh` - Verification tool

## Summary

✅ **Clean entity model** - Explicit types, clear separation
✅ **Proper view identifiers** - group_uuid + view_id (group_uuid:seqno)
✅ **Complete state transitions** - from_state → to_state
✅ **Better performance** - 42% fewer entities
✅ **Backward compatible** - Works with v2 data
✅ **Production ready** - All tests passing

**Key improvement**: No more GCOMM view confusion - pure WSREP views with proper identifiers.
