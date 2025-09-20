# Node Name Mapping Guide

⚠️ **For comprehensive troubleshooting and technical details, see [TROUBLESHOOTING_NODE_DETECTION.md](TROUBLESHOOTING_NODE_DETECTION.md)**

## When Do You Need Manual Node Mapping?

Manual node name specification is needed when log files from different nodes incorrectly identify themselves as the same node, causing visualization issues.

## Quick Check:**
Run the diagnostic script:
```bash
python3 check-node-mapping.py file1.json file2.json
```

**Or check for warnings when running cluster analysis:**
```bash
# Warnings will appear on stderr, JSON stays clean in the file
python3 grambo-cluster.py --format=json file1.json file2.json > output.json
``` Guide

## When Do You Need Manual Node Mapping?

Manual node name specification is needed when log files from different nodes incorrectly identify themselves as the same node, causing visualization issues.

## How to Detect the Problem

**Symptoms:**
- Web UI shows "🧭 Nodes: 1" instead of the expected number of nodes
- Nodes appear to overlap in the visualization
- Missing SST transfer arrows
- Debug messages like "Cannot draw arrow - mapped_donor 'NODE_xyz' in positions: False"

**Quick Check:**
Run the diagnostic script:
```bash
python3 check-node-mapping.py file1.json file2.json
```

## Common Scenarios

### ✅ Good Case (No Manual Mapping Needed)
```bash
# Each file correctly identifies itself
file1.json: local_node_name = "vinfr-db-d-d01"
file2.json: local_node_name = "vinfr-db-d-l05"

# Standard command works:
python3 grambo-cluster.py --format=json file1.json file2.json
```

### ⚠️ Problem Case (Manual Mapping Required)
```bash
# Both files claim to be the same node
file1.json: local_node_name = "vinfr-db-d-l05"  
file2.json: local_node_name = "vinfr-db-d-l05"  # CONFLICT!

# Solution: Use explicit mapping
python3 grambo-cluster.py \
  --node vinfr-db-d-d01:file1.json \
  --node vinfr-db-d-l05:file2.json \
  --format=json
```

## Why This Happens

1. **Log Collection Issues:** Log files might be copied from one node to another
2. **Shared Storage:** Logs stored on shared filesystem with incorrect node identification
3. **Configuration Problems:** Nodes with incorrect `wsrep_node_name` settings
4. **Time Range Overlap:** Same time period from multiple node perspectives

## Best Practices

1. **Always check first:** Use the diagnostic script before cluster analysis
2. **Use descriptive names:** Match node names to your actual cluster topology
3. **Verify in UI:** Check that "🧭 Nodes: X" shows the expected count
4. **File naming:** Use clear filenames like `db01.json`, `db02.json` to avoid confusion

## Diagnostic Script

The `check-node-mapping.py` script automatically:
- Detects node name conflicts
- Suggests correct mapping commands
- Provides ready-to-use command lines
- Returns exit codes for scripting (0=ok, 1=manual mapping needed)
