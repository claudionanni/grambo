# GRAP Validation Quick Reference

## 🚀 One-Command Validation
```bash
./tests/validation/run_validation_suite.sh
```

## 📊 Test Results Summary
| Test | Status | Key Metric |
|------|--------|------------|
| Node State Reliability | ✅ PASSED | 1.89x extraction ratio |
| Pattern Coverage | ✅ PASSED | 90 shifting, 69 membership, 42 other |
| UUID Consistency | ⚠️ WARNING | Multiple UUIDs per node (expected) |
| Entity Coverage | ✅ PASSED | 1179 entities, 8 types |

## 🎯 Quality Indicators
- **Confidence**: 1.0 (maximum) for all entities
- **Coverage**: 8 days, 1150 timestamped events
- **Accuracy**: 56 matching timestamp/node combinations
- **Completeness**: All expected entity types present

## 🔧 Individual Tests
```bash
# Node state reliability
python3 tests/validation/analyze_node_state_reliability.py

# Pattern analysis
python3 tests/validation/analyze_pattern_details.py

# UUID consistency
python3 tests/validation/analyze_uuid_consistency.py

# Entity coverage
python3 tests/validation/analyze_entity_coverage.py
```

## 📁 Generated Reports
- `validation_report_TIMESTAMP.md` - Complete test results
- `NODE_STATE_RELIABILITY_REPORT.md` - Detailed findings
- `VALIDATION_SUITE_OVERVIEW.md` - Complete documentation

## ✅ Pass Criteria
- Node state ratio: 1.5-3.0x ✅
- Timestamp matching: >30 combinations ✅
- Entity coverage: All types present ✅
- Quality metrics: >90% coverage ✅

**Overall Status: PRODUCTION READY** 🎉