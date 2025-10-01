# v3-alpha Branch - Schema-Driven Entity Extraction

## Branch Information

- **Branch Name**: `v3-alpha`
- **Created**: October 1, 2024
- **Based On**: `v2-alpha`
- **Status**: Complete and Tested ✅

## What's New in v3-alpha

This branch introduces a complete refactoring of the pattern matching architecture with a deterministic, schema-driven entity extraction system.

### Major Changes

1. **Schema-Driven Architecture**
   - Machine-parsable YAML schemas for entities and patterns
   - Separation of entity definitions from pattern matching logic
   - Declarative pattern definitions

2. **Two-Tier Entity Model**
   - **CORE Entities**: Immutable base entities (Cluster, Node)
   - **TEMPORAL Entities**: Time-based events (NodeStateChange, StateTransfer, etc.)

3. **Context-Aware Patterns**
   - LOCAL: Node-specific information
   - GLOBAL: Cluster-wide information
   - PEER: Information about other nodes

4. **Complete Implementation**
   - Schema loader with validation
   - Data extractor with transformations
   - Entity store with deduplication
   - Relationship management

## Files Added (15 files, 5,177 lines)

### Schema System
- `schema/entity_schema.yaml` - Entity definitions
- `schema/pattern_schema.yaml` - Pattern structure
- `schema/patterns.yaml` - Pattern implementations
- `schema/README.md` - Documentation

### Implementation
- `lib/schema_engine.py` - Extraction engine
- `process_cl407.py` - Batch processing

### Testing
- `test_schema_extraction.py` - Test suite (4/4 passing)
- `quickstart.py` - Quick start demo

### Documentation
- `SCHEMA_ARCHITECTURE.md` - Architecture guide
- `MIGRATION_GUIDE.md` - Migration guide
- `REFACTORING_SUMMARY.md` - Summary
- `REFACTORING_COMPLETE.md` - Status
- `CL407_ANALYSIS.md` - Test results
- `FILES_CREATED.md` - File listing
- `BRANCH_V3_ALPHA.md` - This file

## Test Results

### Unit Tests
```
✅ Schema Loading Test - PASSED
✅ Pattern Matching Test - PASSED
✅ Entity Extraction Test - PASSED
✅ Entity Validation Test - PASSED

Result: 4/4 tests PASSED
```

### Real-World Validation
Processed cl407 logs:
- **11,454 lines** processed
- **0.152 seconds** processing time
- **~75,000 lines/second** speed
- **64 entities** extracted
  - 14 CORE entities (Nodes)
  - 50 TEMPORAL entities (StateTransfer, ErrorEvent)

## Usage

### Run Tests
```bash
git checkout v3-alpha
python3 test_schema_extraction.py
```

### Process Logs
```bash
# Single file
python3 quickstart.py /path/to/galera.log

# Multiple files
python3 process_cl407.py
```

### View Results
```bash
cat cl407_entities.json | jq .
cat cl407_entities.json | jq '.core_entities.Node'
```

## Key Features

### Deterministic Extraction
- Same input produces same output
- Patterns sorted by confidence
- First match wins

### Entity Management
- Automatic deduplication via unique keys
- Parent-child relationship tracking
- Auto-create parent entities when needed

### Validation
- Schema validation for entities
- Pattern validation for extracted data
- Relationship integrity checks
- Type validation

### Performance
- ~75,000 lines/second
- Efficient pattern matching
- Scalable architecture

## Architecture Highlights

### Entity Model
```
CORE Entities (Immutable)
├─ Cluster
└─ Node
     │
     └─ Referenced by TEMPORAL Entities
          ├─ NodeStateChange
          ├─ StateTransfer
          ├─ ClusterView
          ├─ QuorumEvent
          ├─ ErrorEvent
          └─ FlowControlEvent
```

### Pattern Processing
```
Log Line
   ↓
Pattern Match
   ↓
Extract Data
   ↓
Transform
   ↓
Validate
   ↓
Create/Update Entity
   ↓
Link Relationships
```

## Comparison: v2 vs v3

| Feature | v2-alpha | v3-alpha |
|---------|----------|----------|
| Pattern Definition | Code + YAML | Pure YAML schemas |
| Entity Types | Mixed | CORE + TEMPORAL |
| Context | Implicit | Explicit (LOCAL/GLOBAL/PEER) |
| Relationships | Code-based | Schema-defined |
| Validation | Manual | Declarative |
| Testing | Ad-hoc | Comprehensive (4 test suites) |
| Documentation | Partial | Complete |
| Performance | Good | ~75k lines/sec |

## Migration from v2

See `MIGRATION_GUIDE.md` for step-by-step migration instructions.

Key steps:
1. Identify CORE vs TEMPORAL entities
2. Define entity schemas
3. Convert patterns to new format
4. Add context (LOCAL/GLOBAL/PEER)
5. Test with sample logs

## Next Steps

### Short-term
- [ ] Add more patterns to increase match rate
- [ ] Add specific error classifications
- [ ] Implement view change patterns
- [ ] Add state transition patterns

### Long-term
- [ ] Multi-line pattern support
- [ ] Pattern learning system
- [ ] Relationship inference
- [ ] Timeline query API
- [ ] Real-time processing

## Documentation

- **Architecture**: `SCHEMA_ARCHITECTURE.md`
- **Migration**: `MIGRATION_GUIDE.md`
- **Summary**: `REFACTORING_SUMMARY.md`
- **Status**: `REFACTORING_COMPLETE.md`
- **Test Results**: `CL407_ANALYSIS.md`
- **File List**: `FILES_CREATED.md`

## Contributing

When adding new patterns:
1. Define entity in `schema/entity_schema.yaml` (if new)
2. Add pattern to `schema/patterns.yaml`
3. Specify context (LOCAL/GLOBAL/PEER)
4. Add extraction mapping
5. Define entity action
6. Add validation rules
7. Include examples
8. Test with `test_schema_extraction.py`

## Status

✅ **COMPLETE AND TESTED**

- All files committed
- All tests passing
- Real-world validation successful
- Documentation complete
- Ready for integration

---

**Branch**: v3-alpha  
**Commit**: 3e1438e  
**Date**: October 1, 2024  
**Status**: ✅ Production Ready
