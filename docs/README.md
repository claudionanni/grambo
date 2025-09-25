# Grambo - Galera Log Analysis Suite

A comprehensive suite of tools for analyzing MySQL/MariaDB Galera cluster log files, now featuring a complete **3-tool pipeline** for single-node analysis, multi-node correlation, and interactive web visualization.

## 🔧 GRAP - Enhanced Entity Extraction (Production Ready)

**GRAP** (Galera Real-time Analysis Parser) is the enhanced production implementation featuring:

- **🎯 Entity-Based Architecture** - Extracts structured entities (SST, IST, VIEW, NODE, ERROR, TRANSACTION)
- **� IST Workflow Tracking** - Complete Incremental State Transfer lifecycle monitoring
- **📊 Multi-Format Output** - Text, JSON, and YAML output formats
- **� Intelligent Caching** - Cache analysis results for faster re-processing
- **🌐 Multi-Node Analysis** - Analyze multiple log files with cluster correlation
- **⚡ Advanced Filtering** - Filter by entity types and confidence thresholds

### GRAP Usage
```bash
# Basic single node analysis
./grap galera-node.log

# Multi-node cluster analysis
./grap node1.log node2.log --multi

# JSON output with entity filtering
./grap --format=json --entities=SST,IST galera-node.log

# IST workflow analysis
./grap --entities=IST --format=json galera-node.log

# Hierarchical SST+IST visualization with graa
./grap --entities=SST,IST --format=json galera-node.log | python3 graa --stdin --sst-ist-tree
```

### SST+IST Hierarchical Visualization

GRAP combined with GRAA now provides comprehensive **hierarchical SST+IST relationship trees**:

**Normal SST with backup transfer:**
![SST+IST Tree Example](../img/graa_sst_ist_tree_example.png)

**SST script-only mode with extensive IST processing:**
![SST+IST No Backup Transfer](../img/graa_sst_ist_no_backup_transfer.png)

Features:
- **🔗 Clear Relationships**: Visual mapping between SST sessions and related IST events
- **📊 Progress Tracking**: IST completion percentages and event counts
- **⚡ Status Indicators**: Icons for different SST types and transfer modes
- **🎯 Session Grouping**: Logical organization by time ranges and workflows

**Current Implementation:**
- **`grap`** - Complete entity extraction with IST workflow support
- **`graa`** - Analysis summaries (legacy compatibility)
- **`gras`** - Multi-node correlation (legacy)
- **`graw`** - Web visualization (legacy)

**Entity Types Extracted:**
- **SST** - State Snapshot Transfer operations and lifecycle
- **IST** - Incremental State Transfer with 8-stage workflow tracking  
- **VIEW** - Cluster membership changes and view formations
- **NODE** - State transitions, configuration, membership
- **ERROR** - Error conditions and critical issues
- **TRANSACTION** - Transaction processing events

For detailed documentation, see [README_grap.md](README_grap.md).

### 📁 Repository Structure

- **`master` branch** - Stable original tools (`gra`, `gras`, `graw`)
- **`v2-rewrite` branch** - Modern GRAP system (future main implementation)

## 🔧 Original Tool Pipeline Overview (Legacy - master branch)

The original Grambo consists of three complementary bash-based tools that work together:

1. **`gra`** - Single-node log analysis (legacy version, now replaced by `graa.py`)
2. **`gras`** - Multi-node cluster correlation (grambo state)
3. **`graw`** - Interactive web visualization (grambo web)

### 📊 Complete Analysis Workflow

```bash
# Step 1: Analyze individual node logs
./gra --format=json node1.log > node1.json
./gra --format=json node2.log > node2.json  
./gra --format=json node3.log > node3.json

# Step 2: Correlate cluster-wide events
./gras --format=json node1.json node2.json node3.json > cluster-analysis.json

# Step 3: Launch interactive web visualization
./graw cluster-analysis.json
# Opens browser at http://127.0.0.1:8050
```

### 🌐 Web Visualization Features

The new **`graw`** provides an interactive dashboard with:

- **📈 Timeline Navigation** - Scrub through cluster events chronologically
- **🌐 Network Topology** - Visual cluster state with dynamic node positioning
- **📊 Real-time State Display** - Current cluster members, uncertain nodes, active transfers
- **🔍 Event Details** - Detailed event logs for each timeline frame
- **⚙️ Dynamic Node Management** - Nodes appear only when they interact with the cluster
- **🎯 Temporal Precision** - Accurate timing of node joins, SST workflows, state transitions

#### Visual Elements
- **Green nodes**: SYNCED (healthy)
- **Blue nodes**: DONOR/DESYNCED (providing SST/IST)
- **Orange nodes**: JOINER/JOINING (receiving transfers)
- **Dark orange nodes**: JOINED (synchronized but not yet stable)
- **Node positioning**: Established members (inner circle), uncertain nodes (outer circle), excluded nodes (isolated)

## Overview

Grambo Python Edition provides a clean, organized analysis of Galera cluster logs with separate sections for different types of events. This makes it much easier to understand what's happening in your Galera cluster compared to the verbose output of the original bash version.

## Features

### 🎯 **Organized Event Categories**
- **📊 Server Information** - Version, socket, port, configuration
- **🔗 Galera Cluster Info** - Node UUID, group UUID, cluster details
  - Now with explicit labeling:
    - Node Instance UUID (My UUID): full 36-char per-node identity
    - Group UUID: short 8-4 view/cluster UUID with current seqno
    - Local State: provider state UUID:seqno on this node
  - Local node: resolved node name for this log (when known)
- **🔄 State Transitions** - Node state changes (JOINED→SYNCED, etc.)
- **👥 Cluster Views** - Membership changes, nodes joining/leaving
- **💾 SST Events** - State Snapshot Transfer operations
- **📈 IST Events** - Incremental State Transfer operations
  - Now includes ranges, roles (sender/receiver), async serve peer and preload start
  - Detects receiver prepare/apply start/completion and incomplete ranges
  - NEW: correlates end-to-end State Transfer workflows (IST attempt → SST fallback → IST catch-up)
- **⚠️ Communication Issues** - Node suspicions, network problems
- **⚡ Warnings** - Non-critical issues
- **🚨 Errors** - Critical problems
 - **🛠️ Flow Control** - Summary of FC interval, STOP/CONT signals, SYNC decisions (from gcs.cpp)

### 🔧 **Advanced Features**
- **Multiple output formats**: Human-readable text and JSON
- **Event filtering**: Focus on specific event types
- **Timeline analysis**: Chronological view of cluster events
- **Cross-node analysis**: Analyze multiple log files together
- **Structured data**: Machine-readable JSON for integration

### 🎯 **Dialect Dictionary System**

Grambo Python Edition features a comprehensive **dialect registry** that organizes 80+ regex patterns into logical categories for parsing different Galera/MariaDB/PXC log formats. This system provides:

#### **Pattern Categories**
- **📬 IST Patterns** (20) - Incremental State Transfer events
- **💾 SST Patterns** (18) - State Snapshot Transfer events  
- **🔄 State Transition Patterns** (6) - WSREP state changes
- **👁️ View Change Patterns** (8) - Cluster membership changes
- **🌐 Communication Patterns** (6) - Network and connection events
- **ℹ️ Server Info Patterns** (12) - Version, configuration, and startup
- **⚡ Flow Control Patterns** (5) - Replication flow control
- **🔧 General Patterns** (5) - Timestamps and dialect detection

#### **Current Implementation**
- ✅ **Comprehensive Pattern Registry** - 80+ patterns organized in 8 logical categories
- ✅ **Universal Default Dialect** - Works across all Galera/MariaDB/PXC versions  
- ✅ **Zero breaking changes** - All existing functionality preserved
- ✅ **Automatic detection framework** - Ready for version-specific pattern activation

#### **Future-Ready Extensions**
The dialect system is designed for easy extension:

```python
# Example: Adding MariaDB 10.6 specific patterns
analyzer.dialect_registry.add_dialect_variant('mariadb-10.6')
analyzer.dialect_registry.update_pattern('mariadb-10.6', 'sst_patterns', 
    'enhanced_progress', r'SST progress: (\d+)% \((\d+)/(\d+) MB\)')
```

This enables version-specific parsing improvements:
- **MariaDB 10.6 vs 11.0** - Different log message formats
- **Percona XtraDB Cluster** - PXC-specific terminology  
- **Enterprise vs Community** - Edition-specific patterns
- **Future versions** - Easy pattern updates without code changes

#### **Benefits**
- **🎯 Improved Accuracy** - Version-specific patterns for better parsing
- **🔧 Easy Maintenance** - Centralized pattern management
- **🚀 Community-Friendly** - Simple contribution of dialect-specific patterns
- **📈 Scalable** - Supports unlimited dialect variants

### 🎯 **Enhanced Local Node Detection (Latest)**

Grambo Python Edition now features **robust automatic local node detection** that eliminates the need for the `--node` parameter in most scenarios:

#### **🔧 What's New**
- **✅ UUID Format Handling** - Supports both long (`4bff9935-956b-11f0-9e34-beb439e24709`) and short (`4bff9935-9e34`) UUID formats
- **✅ Node Restart Tracking** - Handles nodes that restart and get new UUIDs, maintaining UUID history
- **✅ Reliable Server Connection Mapping** - Matches "My UUID" lines with "Server connected" events for deterministic name resolution
- **✅ Fixed Processing Bug** - Corrected filter that was skipping server connection lines in `_parse_ip_evidence`
- **✅ Smart Fallback** - Only requests `--node` parameter when automatic detection truly fails

#### **🚀 Before vs After**

**Before (Often Required):**
```bash
# Manual node specification often needed
./gra --node vinfr-db-d-l05 /var/log/mysql/l05.log --format=json > l05.json
./gras --node l05:l05.json --node d01:d01.json
```

**After (Automatic):**
```bash
# Auto-detection works reliably
./gra /var/log/mysql/l05.log --format=json > l05.json
./gras l05.json d01.json  # No --node needed!
```

#### **🧠 How It Works**

1. **UUID Collection**: Tracks all UUIDs from "My UUID" declarations and server connection events
2. **Format Conversion**: Automatically converts between long and short UUID formats for matching
3. **Historical Tracking**: Maintains UUID history when nodes restart and get new identifiers  
4. **Reliable Mapping**: Uses deterministic UUID→name mappings from server connection logs
5. **Validation**: Only succeeds when reliable detection is possible, fails safely otherwise

#### **🎯 When --node Is Still Needed**

- **Severely truncated logs** lacking server connection information
- **Custom naming requirements** (display names different from Galera node names)
- **Very old logs** that predate reliable detection patterns
- **Mixed log files** containing multiple node perspectives

#### **💡 Benefits**

- **🚀 Simplified Workflow** - No more guessing node names from log content
- **📊 Better gras** - Automatic node detection cascades to cluster analysis
- **🔧 Fewer Errors** - Eliminates manual node name typos and mismatches
- **⚡ Faster Analysis** - Skip the trial-and-error of determining correct node names

**📖 Detailed Technical Documentation**: See [CHANGELOG_NODE_DETECTION.md](CHANGELOG_NODE_DETECTION.md) for complete technical details, test cases, and migration guide.

## Installation

```bash
# Clone the repository
git clone https://github.com/claudionanni/grambo.git
cd grambo

# The tools are ready to use
chmod +x gra gras graw

# Install dependencies for web visualization (optional)
pip install dash plotly pandas networkx
```

## Usage

### 🚀 Quick Start - Complete Pipeline

```bash
# 1. Analyze individual Galera node logs
./gra --format=json /var/log/mysql/node1-error.log > node1.json
./gra --format=json /var/log/mysql/node2-error.log > node2.json
./gra --format=json /var/log/mysql/node3-error.log > node3.json

# 2. Generate cluster-wide analysis
./gras --format=json node1.json node2.json node3.json > cluster-analysis.json

# 3. Launch interactive web dashboard
./graw cluster-analysis.json
# Visit http://127.0.0.1:8050 in your browser
```

### 📋 Tool-Specific Usage

#### 1. Single-Node Analysis (`gra`)

The --mariadb-version and --mariadb-edition parameters are there to keep the tool open to multiple intepretations of the logs which we have seen changing format along the years.

##### Analyze a log file (recommended: specify MariaDB version and edition)
```bash
./gra --mariadb-version 11.4 --mariadb-edition enterprise /var/log/mysql/error.log
```

##### For MariaDB Community edition
```bash
./gra --mariadb-version 10.6 --mariadb-edition community /var/log/mysql/error.log
```

##### You can also use stdin
```bash
cat /var/log/mysql/error.log | ./gra --mariadb-version 11.4 --mariadb-edition enterprise
```

##### Without the above parameters it'll try to get them from the log, if available
```bash
# Analyze a log file
./gra /var/log/mysql/error.log

# Using stdin
cat /var/log/mysql/error.log | ./gra

# Make it executable and use directly
./gra /var/log/mysql/error.log
```

#### 2. Multi-Node Cluster Analysis (`gras`)

```bash
# Basic cluster analysis (enhanced auto-detection often eliminates need for --node parameters)
./gras node1.json node2.json node3.json

# JSON output for web visualization
./gras --format=json node1.json node2.json node3.json > cluster.json

# Manual node mapping (only needed when auto-detection fails)
./gras --node-names db1,db2,db3 node1.json node2.json node3.json

# Alternative syntax with explicit mapping
./gras --node db1:node1.json --node db2:node2.json --node db3:node3.json
```

**💡 Note**: With the enhanced node detection in `gra`, explicit node mapping is now rarely needed. The cluster analyzer will automatically extract node names from the JSON files' `local_node_name` fields.

#### 3. Interactive Web Visualization (`graw`)

```bash
# Launch web dashboard (default port 8050)
./graw cluster-analysis.json

# Custom port
./graw cluster-analysis.json --port 8051

# The dashboard will be available at http://127.0.0.1:PORT
```

### Advanced Options

#### gra Options
```bash
# JSON output for integration with other tools
./gra --format=json error.log

# Filter specific event types
./gra --filter=sst_event,state_transition error.log

# Filter multiple types (comma-separated)
./gra --filter=error,warning error.log

# Combine options
./gra --format=json --filter=cluster_view error.log

# Provide MariaDB / Galera version info explicitly (recommended if version lines missing)
./gra --mariadb-version 11.4.7 --mariadb-edition=community error.log
./gra --mariadb-version 11.4.7 --mariadb-edition=enterprise error.log
./gra --mariadb-version 10.6.16 --galera-version 26.4.23 error.log
```

#### gras Options
```bash
# Quiet mode (minimal output)
./gras --quiet node1.json node2.json node3.json

# Time range filtering
./gras --start-time "2025-09-19 10:00:00" --end-time "2025-09-19 12:00:00" *.json

# Focus on specific event types
./gras --events sst,state_transition *.json
```

#### graw Options
```bash
# Custom port and host
./graw cluster.json --port 8080 --host 0.0.0.0

# Debug mode
./graw cluster.json --debug
```

## 🔍 Cluster Analysis Features

### Multi-Node Correlation
- **SST Workflow Tracking** - Correlates joiner requests with donor responses across nodes
- **Split-Brain Detection** - Identifies when nodes have different cluster views
- **Timeline Synchronization** - Aligns events across all nodes chronologically
- **State Transition Analysis** - Tracks node state changes cluster-wide

### Web Dashboard Capabilities
- **Interactive Timeline** - Navigate through cluster events frame by frame
- **Dynamic Network Topology** - Visual representation of cluster state at any point in time
- **Node Classification** - Automatic categorization of nodes (established/uncertain/excluded)
- **Temporal Precision** - Nodes appear only when they actually interact with the cluster
- **Event Correlation** - Links related events across different nodes

### Real-World Scenarios Supported
- **Node Bootstrap** - Visualize how nodes join an existing cluster
- **Rolling Restarts** - Track state transitions during maintenance
- **Network Partitions** - Identify split-brain scenarios and recovery
- **SST/IST Analysis** - Deep-dive into state transfer workflows
- **Performance Issues** - Correlate timing issues across cluster members

### Command Line Flags

| Flag | Description | Example |
|------|-------------|---------|
| `--format` | Output format (`text` or `json`) | `--format=json` |
| `--filter` | Comma-separated event types to include | `--filter=sst_event,state_transition` |
| (deprecated) `--dialect` | Ignored; dialect auto-detected from MariaDB version/edition | — |
| `--report-unknown` | Include unclassified WSREP/IST lines summary | `--report-unknown` |
| `--mariadb-version` | Manually supply MariaDB server version if log lacks version banner | `--mariadb-version 11.4.7` |
| `--mariadb-edition` | Specify edition (`enterprise` or `community`) for variant tagging | `--mariadb-edition enterprise` |
| (deprecated) `--galera-version` | Ignored; provider version inferred or parsed automatically | — |

### Version Inference

If the log contains standard startup lines (e.g. `Server version:` or `wsrep_load(): Galera 26.4.xx by Codership Oy`) the analyzer auto-detects versions. Manual override flags `--dialect` and `--galera-version` are deprecated and ignored; keep `--mariadb-version` / `--mariadb-edition` if banners missing.

Current built-in Galera inference (when `--galera-version` omitted but MariaDB version provided):

| MariaDB Series | Inferred Galera Version |
|----------------|-------------------------|
| 10.6.x | 26.4.22 |
| 11.4.x | 26.4.23 |

If inference occurs, the text report marks Galera as `(inferred)` unless an actual provider banner is later parsed.

### Dialect Detection and Pattern Selection

The analyzer automatically detects the dialect from log content:

1. **Automatic Detection**: Scans for version banners and provider information
2. **Pattern Selection**: Uses appropriate patterns for detected MariaDB/Galera version
3. **Fallback Safety**: Unknown or undetected dialects use default patterns
4. **Manual Override**: Use `--mariadb-version` and `--mariadb-edition` for explicit control

**Current Status:**
- **Default Dialect Only** - Currently uses universal patterns that work with all Galera versions
- **Pattern Registry Ready** - System supports dialect-specific patterns but uses only default currently
- **Future Extensions** - Framework ready for MariaDB 10.6/11.4, Percona XtraDB Cluster patterns

The dialect system currently uses default patterns for all environments, ensuring broad compatibility while providing the foundation for version-specific optimizations.

### Deprecated Flags

| Flag | Status | Action |
|------|--------|--------|
| `--dialect` | Ignored | Remove from scripts; rely on auto-detection |
| `--galera-version` | Ignored | Provide `--mariadb-version` if startup snippet truncated |

Supplying these prints a warning and has no effect.

Supplying `--mariadb-edition enterprise` will also tag Galera variant as enterprise unless contradicted by a parsed provider path.

Examples:

```bash
# Log snippet without early startup lines
grep -v 'Server version' truncated.log | ./gra --mariadb-version 11.4.7 --mariadb-edition community

# Force a specific Galera provider version (overrides inference)
./gra --mariadb-version 10.6.16 --galera-version 26.4.23 db3.log
```

### Available Event Types for Filtering

- `server_info` - Server configuration and startup information
- `cluster_view` - Cluster membership changes  
- `state_transition` - Node state transitions
- `sst_event` - State Snapshot Transfer events
- `ist_event` - Incremental State Transfer events
- `communication` - Communication problems
- `warning` - Warning messages
- `error` - Error messages

## Example Output

### Single-Node Analysis (gra)

The following is a sanitized example. Replace values with those from your environment.

#### Text Format (Default)
```
================================================================================
| G R A M B O - Galera Log Deforester (Python Edition)
================================================================================

📊 SERVER INFORMATION
--------------------------------------------------
  Version: 10.6.x-MariaDB
  Socket: /run/mysqld/mysqld.sock
  Port: 3306
  Address: 10.0.0.3

🔗 GALERA CLUSTER INFORMATION
--------------------------------------------------
  Galera Version: 26.4.xx
  Node UUID: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
  Group UUID: bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
  Group Name: my_wsrep_cluster

🧭 GROUP STATE
--------------------------------------------------
  Group UUID: bbbbbbbb-bbbb (seqno: 22)
  Local State: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1776
  Node Instance UUID (My UUID): aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
  Local node: node-03


🔄 STATE TRANSITIONS
--------------------------------------------------
  2025-09-15 13:50:35 | JOINED → SYNCED
    └─ Sequence: 1776

💾 STATE SNAPSHOT TRANSFER (SST)
--------------------------------------------------
  2025-09-15 13:45:56 | SST REQUEST
    └─ Method: mariabackup
    └─ Donor: 10.0.0.2
    └─ Joiner: 10.0.0.3
  2025-09-15 13:48:11 | SST FAILED

🛠️  FLOW CONTROL
--------------------------------------------------
  Interval: [102, 128] (last seen 2025-09-15 13:53:05)
  FC_STOP sent: 3 | FC_CONT sent: 3
  SYNC decisions — sent: 1, not sent: 2

🧩 STATE TRANSFER WORKFLOWS
--------------------------------------------------
Request 2025-09-15 13:50:43: node-01 ⇐ node-03
  SST: started at 2025-09-15 13:50:43
  Post-IST: async serve tcp://10.0.0.2:4568 1726→1810 at 2025-09-15 13:53:06
```

### Multi-Node Cluster Analysis (gras)

```
================================================================================
| G R A M B O - GALERA CLUSTER MULTI NODE LOG ANALYZER
================================================================================

🌐 CLUSTER OVERVIEW
--------------------------------------------------
  Nodes: NODE_50000, NODE_54320, NODE_54321
  Time Range: 2025-09-18 14:18:09 - 2025-09-19 11:12:24
  Total Events: 202

🔄 SST/IST WORKFLOWS
--------------------------------------------------
  2025-09-18 15:48:41 | NODE_54320 → NODE_54321 | STARTED (mariabackup)
  2025-09-19 11:10:02 | NODE_54320 → NODE_54321 | STARTED (mariabackup)
  2025-09-19 11:10:39 | NODE_50000 → NODE_54320 | REQUESTED (mariabackup)

⚠️  SPLIT-BRAIN SCENARIOS
--------------------------------------------------
  2025-09-19 11:10:00 | Different cluster views:
    └─ NODE_54320: {NODE_54320, NODE_54321}
    └─ NODE_54321: {NODE_54320, NODE_54321}
    └─ NODE_50000: {NODE_50000, NODE_54320, NODE_54321}

🔄 STATE TRANSITIONS
--------------------------------------------------
  2025-09-18 15:48:40 | NODE_54320 | CLOSED → OPEN (seqno: 0)
  2025-09-18 15:48:40 | NODE_54320 | OPEN → PRIMARY (seqno: 18)
  2025-09-18 15:48:41 | NODE_54320 | PRIMARY → JOINER (seqno: 18)
  2025-09-19 11:10:39 | NODE_50000 | PRIMARY → JOINER (seqno: 3)
  2025-09-19 11:12:24 | NODE_50000 | JOINER → JOINED (seqno: 5)
  2025-09-19 11:12:24 | NODE_50000 | JOINED → SYNCED (seqno: 5)
```

### Interactive Web Dashboard (graw)

The web dashboard provides:

1. **Timeline Slider** - Navigate through cluster events chronologically
2. **Network Graph** - Visual cluster topology with color-coded node states
3. **Current State Panel** - Real-time cluster status including:
   - Cluster members (established nodes)
   - Uncertain nodes (transitioning/joining)
   - Active transfers (SST/IST operations)
4. **Event Log** - Detailed event information for the current timeline frame

#### Visual State Indicators
- **🟢 Green**: SYNCED (healthy, operational)
- **🔵 Blue**: DONOR/DESYNCED (providing state transfer)
- **🟠 Orange**: JOINER/JOINING (receiving state transfer)
- **🟤 Dark Orange**: JOINED (synchronized, stabilizing)
- **🔴 Red**: ERROR/CLOSED (problematic states)
- **⚫ Gray**: UNKNOWN/disconnected

### JSON Format
```json
{
  "server_info": {
    "version": "10.6.x-MariaDB",
    "socket": "/run/mysqld/mysqld.sock",
    "port": "3306",
    "address": "10.0.0.3"
  },
  "ist_summary": {
    "receiver": {
      "prepared_range": {
        "first_seqno": 1726,
        "last_seqno": 1810,
        "listen_addr": "tcp://10.0.0.3:4568",
        "timestamp": "2025-09-15 13:53:06"
      },
      "completed_at": "2025-09-15 13:53:07"
    },
    "sender": {
      "ranges": [
        { "first_seqno": 1726, "last_seqno": 1810, "timestamp": "2025-09-15 13:53:06" }
      ],
      "async": [
        { "peer": "tcp://10.0.0.2:4568", "first_seqno": 1726, "last_seqno": 1810, "preload_start": 1726, "timestamp": "2025-09-15 13:53:06" }
      ],
      "failures": []
    },
    "counts": { "total": 12, "sender_ranges": 4, "async_starts": 4, "failures": 0 }
  },
  "st_workflows": [
    {
      "requested_at": "2025-09-15 13:50:43",
      "joiner": "node-01",
      "donor": "node-03",
      "pre_ist_signals": [],
      "sst": { "timestamp": "2025-09-15 13:50:43", "status": "started" },
      "post_ist": {
        "async_start": {
          "timestamp": "2025-09-15 13:53:06",
          "peer": "tcp://10.0.0.2:4568",
          "first_seqno": 1726,
          "last_seqno": 1810
        },
        "completed_at": "2025-09-15 13:53:07"
      }
    }
  ],
  "events": [
    {
      "timestamp": "2025-09-15 13:50:35",
      "event_type": "state_transition",
      "details": {
        "from_state": "JOINED",
        "to_state": "SYNCED",
        "sequence_number": "1776"
      }
    }
  ],
  "summary": {
    "total_events": 59,
    "events_by_type": {
      "sst_event": 7,
      "ist_event": 12,
      "warning": 30,
      "error": 10
    }
  }
}
```

## Understanding Galera Events

### State Transitions
Galera nodes go through various states:
- **JOINING** → **JOINED** → **SYNCED** → **DONOR** (normal flow)
- **SYNCED** is the healthy operational state
- **DONOR** means the node is providing SST/IST to other nodes

### SST vs IST
- **IST (Incremental State Transfer)**: First attempt when a node needs to resync; donor serves missing write sets from gcache. If gcache doesn’t contain the full required range or IST isn’t possible, it falls back to SST.
- **SST (State Snapshot Transfer)**: Full resync via wsrep_sst_mariabackup (default). Donor runs mariabackup and streams to the joiner on port 4568; the joiner wipes datadir, restores and prepares the backup, then starts MariaDB. After SST, a short IST catch-up typically follows.

### Cluster Views
Track which nodes are members of the cluster at any given time, including:
- Nodes that joined the cluster
- Nodes that left gracefully  
- Nodes that were partitioned (network split)

## Requirements

### Core Analysis Tools (gra, gras)
- Python 3.7 or higher
- No external dependencies required

### Web Visualization (graw)
- Python 3.7 or higher
- `dash` - Web application framework
- `plotly` - Interactive plotting library  
- `pandas` - Data manipulation
- `networkx` - Network graph algorithms

```bash
# Install web dashboard dependencies
pip install dash plotly pandas networkx

# Or using a virtual environment (recommended)
python3 -m venv grambo-env
source grambo-env/bin/activate
pip install dash plotly pandas networkx
```

## 🚨 Troubleshooting

### Node Name Conflicts

If you see warnings like `⚠️ WARNING: Duplicate node name detected!` or notice:
- Web UI shows fewer nodes than expected (e.g., "🧭 Nodes: 1" instead of "🧭 Nodes: 2")
- Nodes appear overlapped in visualization
- Missing SST transfer arrows

This happens when multiple log files incorrectly identify themselves as the same node.

**Quick Fix:**
```bash
# Check for conflicts first
python3 check-node-mapping.py node1.json node2.json

# Use explicit node mapping if conflicts detected
./gras \
  --node actual-node-01:node1.json \
  --node actual-node-02:node2.json \
  --format=json > cluster-analysis.json
```

**Why This Happens:**
- Log files from different nodes may contain ambiguous node identification
- Centralized log collection can lose original node context
- Missing or incorrect `wsrep_node_name` configuration
- Multi-perspective logging where same events appear in multiple node logs

**Detailed Guide:** See [TROUBLESHOOTING_NODE_DETECTION.md](TROUBLESHOOTING_NODE_DETECTION.md) for comprehensive explanation and solutions.

### Diagnostic Tools

**Node Mapping Checker:**
```bash
python3 check-node-mapping.py *.json
# Automatically detects conflicts and suggests fixes
```

**Silent Operation:**
```bash
# Suppress all warnings for automated scripts
./gras --format=json --quiet file1.json file2.json > output.json
```

## Development

The code is organized into clear classes and functions:
- `GaleraLogParser`: Main parsing logic (gra)
- `ClusterAnalyzer`: Multi-node correlation engine (gras)  
- `WebClusterVisualizer`: Interactive dashboard (graw)
- Event-specific parsers for each type of Galera event
- Modular regex patterns for easy maintenance

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample Galera logs
5. Submit a pull request

## Legacy Compatibility

The original bash grambo is still available as `grambo` (without .py extension). The Python suite provides the same analysis with much better organization, multi-node correlation, and interactive visualization.

## License

Same license as the original grambo project.
