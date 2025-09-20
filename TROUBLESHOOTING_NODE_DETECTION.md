# Node Name Detection and Conflicts in Galera Log Analysis

## Overview

When analyzing Galera cluster logs with Grambo, you may encounter situations where multiple log files incorrectly identify themselves as the same node, causing visualization issues like overlapping nodes and missing SST transfer arrows. This document explains why this happens and how to resolve it.

## The Problem

### Symptoms
- Web UI shows fewer nodes than expected (e.g., "🧭 Nodes: 1" instead of "🧭 Nodes: 2")
- Nodes appear overlapped in cluster visualization
- Missing SST transfer arrows between nodes
- Debug messages: `Cannot draw arrow - mapped_donor 'NODE_xyz' in positions: False`

### Root Cause
Grambo's `gramboo.py` script attempts to automatically detect which Galera node generated each log file by analyzing log content. However, this detection can fail or be ambiguous, leading to multiple log files claiming to be from the same node.

## Why Node Name Detection Fails

### 1. **Log Content Ambiguity**

Galera logs don't always clearly identify their source node. `gramboo.py` looks for patterns like:

```
2024-09-19 18:17:17 Member 0.0 (node-name) requested state transfer from ...
2024-09-19 18:33:48 Server synced with group: node-name
```

**Problems:**
- **Missing patterns:** Some logs lack these identifying markers
- **Inconsistent naming:** Node names may appear differently in various log sections
- **Placeholder names:** Logs might use temporary identifiers like "Temp-uuid" instead of real names

### 2. **Log Collection Issues**

**Centralized Logging:**
```bash
# Common scenario causing confusion
scp db-node-01:/var/log/mysql/error.log ./db01.log
scp db-node-02:/var/log/mysql/error.log ./db02.log
# Both logs might reference the same "primary" node in cluster events
```

**Shared Storage:**
- Logs written to shared NFS/CIFS mounts can lose original context
- Container orchestration may create ambiguous log paths
- Log rotation/archival systems may mix node identities

### 3. **Galera Configuration Problems**

**Missing `wsrep_node_name`:**
```sql
-- Problematic: No explicit node naming
SHOW VARIABLES LIKE 'wsrep_node_name';
-- Empty or default value

-- Correct: Explicit node identification  
SET GLOBAL wsrep_node_name = 'db-node-01';
```

**Identical Hostnames:**
- Docker containers with same base image
- Kubernetes pods with generated names
- VM templates without customization

### 4. **Multi-Perspective Events**

Galera cluster events are logged from multiple perspectives:

```
# Node A's log (vinfr-db-d-d01):
2024-09-19 18:17:17 Member 0.0 (vinfr-db-d-l05) requested state transfer

# Node B's log (vinfr-db-d-l05):  
2024-09-19 18:17:17 Requesting state transfer from Member 1.1 (vinfr-db-d-d01)
```

When analyzing overlapping time periods, both logs mention both nodes, making automatic detection difficult.

## How gramboo.py Detects Node Names

### Detection Logic (in order of preference)

1. **SST Request Pattern:**
   ```python
   # Pattern: Member X.X (node-name) requested state transfer
   m_local = re.search(r'Member\s+\d+\.\d+\s+\(([A-Za-z0-9_.-]+)\)\s+requested state transfer', line)
   ```

2. **Server Sync Pattern:**
   ```python
   # Pattern: Server synced with group: node-name
   if 'Server synced with group' in line:
       self.cluster.local_node_name = extracted_name
   ```

3. **State Transition Events:**
   ```python
   # Various wsrep state change patterns
   if 'wsrep_local_state_comment' in line:
       # Extract node identity from state transitions
   ```

4. **Fallback to Filename:**
   ```python
   # Last resort: use filename as node identifier
   base_name = os.path.basename(file_path)
   node_name = base_name.split('.')[0]  # db01.json -> db01
   ```

### Why Detection Fails

**Ambiguous Log Content:**
- Logs from SST donor nodes often contain patterns for both donor and joiner
- Cluster view changes mention all nodes, not just the local one
- Network partition events show multiple node perspectives

**Timing Issues:**
- Early log entries may lack sufficient context
- Node joins/leaves create temporary naming inconsistencies
- SST events create complex donor/joiner relationship logs

## Resolution Strategies

### 1. **Automatic Detection (grambo-cluster.py)**

The cluster analyzer now automatically detects conflicts:

```bash
$ python3 grambo-cluster.py --format=json node1.json node2.json
⚠️  WARNING: Duplicate node name 'vinfr-db-d-l05' detected!
   This might cause visualization issues (overlapping nodes, missing arrows)
   Consider using explicit mapping: --node actual_name:node2.json
```

### 2. **Pre-Analysis Checking**

Use the diagnostic script before cluster analysis:

```bash
$ python3 check-node-mapping.py node1.json node2.json
🔍 CHECKING NODE MAPPING REQUIREMENTS
==================================================
📁 node1.json:
   Local node name: vinfr-db-d-l05
   Visible nodes: ['vinfr-db-d-d01', 'vinfr-db-d-l05']

📁 node2.json:
   Local node name: vinfr-db-d-l05  # ⚠️ CONFLICT!
   Visible nodes: ['vinfr-db-d-d01', 'vinfr-db-d-l05']

⚠️  CONFLICT DETECTED: Duplicate local node names!
🔧 SOLUTION: Use manual node mapping with --node
   python3 grambo-cluster.py --node vinfr-db-d-d01:node1.json --node vinfr-db-d-l05:node2.json --format=json
```

### 3. **Manual Node Mapping**

When automatic detection fails, explicitly specify node identities:

```bash
# Explicit mapping overrides automatic detection
python3 grambo-cluster.py \
  --node db-node-01:logs/server1.json \
  --node db-node-02:logs/server2.json \
  --node db-node-03:logs/server3.json \
  --format=json > cluster-analysis.json
```

### 4. **Prevention at Source**

**Proper Galera Configuration:**
```sql
-- Set explicit node names
SET GLOBAL wsrep_node_name = 'db-prod-01';
SET GLOBAL wsrep_node_address = '10.0.1.10';

-- Verify configuration
SHOW VARIABLES LIKE 'wsrep_node_%';
```

**Structured Log Collection:**
```bash
# Clear naming convention
mkdir cluster-logs/
scp db01:/var/log/mysql/error.log cluster-logs/db01.json
scp db02:/var/log/mysql/error.log cluster-logs/db02.json

# Process with clear mapping
python3 gramboo.py --format=json cluster-logs/db01.log > db01.json
python3 gramboo.py --format=json cluster-logs/db02.log > db02.json
```

## Best Practices

### 1. **Always Verify Node Count**
```bash
# Check expected vs actual node count
jq '.cluster_analysis.nodes | length' cluster-analysis.json
# Should match your actual cluster size
```

### 2. **Use Diagnostic Tools**
```bash
# Pre-flight check
python3 check-node-mapping.py *.json

# Monitor for warnings
python3 grambo-cluster.py --format=json *.json > analysis.json
# Watch stderr for warnings
```

### 3. **Validate Visualization**
- Web UI should show correct node count: "🧭 Nodes: N"
- Nodes should not overlap in cluster diagram
- SST arrows should appear between donor/joiner nodes

### 4. **Document Your Mapping**
```bash
# Create a mapping file for complex topologies
cat > node-mapping.txt << EOF
# Physical to logical node mapping
db-server-prod-01.json -> galera-primary
db-server-prod-02.json -> galera-secondary  
db-server-prod-03.json -> galera-tertiary
EOF

# Use consistent mapping
python3 grambo-cluster.py \
  --node galera-primary:db-server-prod-01.json \
  --node galera-secondary:db-server-prod-02.json \
  --node galera-tertiary:db-server-prod-03.json \
  --format=json
```

## Summary

Node name conflicts in Galera log analysis are caused by:
- **Ambiguous log content** that doesn't clearly identify the source node
- **Collection methodology** that loses original node context
- **Configuration issues** with missing or incorrect node naming
- **Multi-perspective logging** where events appear in multiple node logs

The Grambo toolchain provides multiple layers of detection and resolution:
- **Automatic warnings** in `grambo-cluster.py`
- **Pre-analysis diagnosis** with `check-node-mapping.py`  
- **Manual override capability** with `--node` mapping
- **Clean JSON output** with warnings on stderr

Following proper Galera configuration practices and using explicit node mapping when needed ensures accurate cluster analysis and visualization.
