# GRAP Quality Validation Test Suite

This directory contains validation scripts and reports for verifying the reliability and accuracy of GRAP's entity extraction capabilities.

## Test Scripts

### `analyze_node_state_reliability.py`
**Purpose**: Validates GRAP's node_state entity extraction by comparing with raw log "shifting" events.

**Usage**:
```bash
cd /home/claudio/Projects/GITHUB/grambo
python3 tests/validation/analyze_node_state_reliability.py
```

**What it tests**:
- Accuracy of node state detection
- Coverage comparison between raw logs and GRAP extraction
- Timestamp precision
- Node identification reliability

**Expected output**: Statistical comparison showing ~2x entity ratio (GRAP finds more states than just shifting events)

### `analyze_pattern_details.py`
**Purpose**: Detailed analysis of GRAP's pattern usage and extraction methods.

**Usage**:
```bash
cd /home/claudio/Projects/GITHUB/grambo
python3 tests/validation/analyze_pattern_details.py
```

**What it tests**:
- Pattern distribution across different extraction methods
- Duplicate entity detection
- Source categorization (shifting vs membership vs other patterns)
- Pattern reliability assessment

**Expected output**: Breakdown of extraction methods and duplicate analysis

### `analyze_uuid_consistency.py`
**Purpose**: Validates UUID assignment consistency across time and entity types.

**Usage**:
```bash
cd /home/claudio/Projects/GITHUB/grambo
python3 tests/validation/analyze_uuid_consistency.py
```

**What it tests**:
- UUID consistency for each node over time
- Correlation between node_state and view entity UUIDs
- UUID timeline generation
- Temporal UUID assignment patterns

**Expected output**: UUID consistency report with timeline analysis

### `analyze_entity_coverage.py`
**Purpose**: Comprehensive entity extraction coverage and quality validation.

**Usage**:
```bash
cd /home/claudio/Projects/GITHUB/grambo
python3 tests/validation/analyze_entity_coverage.py
```

**What it tests**:
- Entity type distribution and expected coverage
- Quality metrics (confidence, timestamps, raw lines)
- Temporal coverage analysis
- Unexpected entity type detection

**Expected output**: Entity coverage report with quality assessment

## Test Reports

### `NODE_STATE_RELIABILITY_REPORT.md`
Comprehensive analysis report documenting GRAP's node_state extraction reliability.

**Key findings**:
- ✅ Excellent reliability (all tests passed)
- ✅ Comprehensive state detection from multiple sources
- ✅ Perfect node identification
- ⚠️ Minor duplicate handling opportunities

## Prerequisites

### Required Files
- `cl407/error.11407.log` - Main cluster node log
- `cl407/error.21407.log` - Secondary node log  
- `cl407/error.31407.log` - Tertiary node log
- `grax_output/grap_output.json` - GRAP extraction results

### Required Tools
- Python 3.x with json, subprocess modules
- `grep` command line tool
- `jq` for JSON processing (optional, for manual verification)

## Running All Validation Tests

```bash
#!/bin/bash
# Run complete validation suite

cd /home/claudio/Projects/GITHUB/grambo

echo "=== GRAP Quality Validation Suite ==="
echo "Running node state reliability analysis..."
python3 tests/validation/analyze_node_state_reliability.py

echo -e "\n=== Running detailed pattern analysis ==="
python3 tests/validation/analyze_pattern_details.py

echo -e "\n=== Running UUID consistency validation ==="
python3 tests/validation/analyze_uuid_consistency.py

echo -e "\n=== Running entity coverage analysis ==="
python3 tests/validation/analyze_entity_coverage.py

echo -e "\n=== Validation complete. See NODE_STATE_RELIABILITY_REPORT.md for detailed findings ==="
```

**Or use the automated suite runner**:
```bash
cd /home/claudio/Projects/GITHUB/grambo
./tests/validation/run_validation_suite.sh
```

## Test Data Requirements

The validation scripts expect to find cluster log files in the `cl407/` directory:
- Multi-node Galera cluster logs
- Logs containing WSREP state transition messages
- Time span covering various cluster operations (joins, leaves, SST, etc.)

## Interpretation Guidelines

### Expected Results
- **Node state coverage**: GRAP should extract 1.5-2x more entities than raw shifting events
- **Pattern distribution**: ~45% shifting, ~35% membership, ~20% other patterns
- **Confidence scores**: All entities should have confidence = 1.0
- **Duplicate rate**: ~20-30% of timestamps may have multiple entities (normal)

### Warning Signs
- Zero matching timestamp/node combinations
- Confidence scores below 1.0
- Missing node identification
- Pattern extraction failures

## Future Enhancements

1. **Automated regression testing**: Integration with CI/CD pipeline
2. **Performance benchmarking**: Extraction speed and memory usage validation
3. **Cross-cluster validation**: Test with different Galera versions and configurations
4. **Edge case testing**: Validation with corrupted or incomplete log files

## Contributing

When adding new validation tests:

1. Follow the naming convention: `analyze_[feature]_[aspect].py`
2. Include comprehensive documentation and usage examples
3. Generate summary reports in Markdown format
4. Test with multiple cluster configurations
5. Document expected results and warning signs