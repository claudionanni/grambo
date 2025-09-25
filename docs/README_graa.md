# GRAA - Galera Log Analysis Summary Tool

`graa.py` is a structured log analysis tool that processes `grap.py` output to provide clean, comprehensive summaries of Galera cluster logs.

## Usage

### Direct Log Analysis
```bash
# Analyze a raw Galera log file (calls grap.py internally)
python3 graa.py galera-node.log
```

### Pipeline Mode
```bash
# Use with grap.py in pipeline mode
python3 grap.py --format=json galera-node.log | python3 graa.py --stdin
```

### Options
```bash
python3 graa.py --help           # Show help
python3 graa.py --version        # Show version
python3 graa.py --sst-sessions   # Show detailed SST sessions timeline
python3 graa.py --sst-ist-tree   # Show hierarchical SST+IST relationship tree
```

## SST+IST Hierarchical Tree

The `--sst-ist-tree` option provides a comprehensive hierarchical view of SST (State Snapshot Transfer) and IST (Incremental State Transfer) relationships:

```bash
# Show hierarchical SST+IST tree visualization
./grap logfile.log --entities=SST,IST --format=json | python3 graa --stdin --sst-ist-tree
```

### Tree Visualization Features
- **📊 Session Grouping**: SST events grouped into logical sessions with time ranges
- **🔗 Relationship Mapping**: IST events clearly linked to their corresponding SST sessions
- **📈 Progress Tracking**: IST progress percentages and event counts displayed
- **🎯 Visual Hierarchy**: Tree structure with proper indentation and visual indicators
- **⚡ Status Indicators**: Icons showing SST types (🚀 active, 📋 script-only, ❓ status changes) and IST events (📥)

### Example Output

#### Normal SST with Backup Transfer
![SST+IST Tree Example](../img/graa_sst_ist_tree_example.png)

This shows a typical scenario where SST completes with actual backup transfer, followed by minimal IST processing.

#### SST Script-Only Mode with Extensive IST
![SST+IST No Backup Transfer](../img/graa_sst_ist_no_backup_transfer.png)

This demonstrates a scenario where SST initiates without backup transfer (script-only mode), requiring extensive IST processing to synchronize the node. Key features shown:
- **⚠️ Warning**: "Backup Transfer: NO (script only)"
- **Extensive IST Processing**: Multiple progress updates from 28.4% to 100.0%
- **Large Event Counts**: Processing hundreds of thousands of events (303,719 total)
- **Performance Insights**: Timeline showing IST processing duration and throughput

### Tree Structure Format
```
[SESSION_NUMBER] SST SESSION
    Time Range: start_time → end_time
    ├─ 🚀 SST entity_name │ Status │ Method │ Backup Transfer status
    └─ 📋 SST entity_name │ Status │ Transfer complete
    │
    ├─ 🔄 RELATED IST EVENTS:
    │      ├─ 📥 IST entity_name │ Status │ Time │ Range │ Progress: X% (events)
    │      └─ 📥 IST entity_name │ Status │ Time │ Progress: 100.0% (final_count)
```

### Benefits
- **Troubleshooting**: Quickly identify SST/IST workflow issues
- **Performance Analysis**: Monitor state transfer duration and progress
- **Operational Insight**: Understand backup transfer vs IST processing patterns
- **Documentation**: Clear visual representation for cluster behavior analysis

## SST Sessions Timeline

The `--sst-sessions` option provides a detailed chronological view of SST (State Snapshot Transfer) sessions:

```bash
# Show detailed SST timeline
python3 graa.py --sst-sessions galera-node.log
```

### Features
- **Real Node Names**: Extracts actual node names from log patterns (e.g., UAT-DB-01, NODE_11407)
- **Accurate Timing**: Calculates precise durations (e.g., 2m 15s, 16m 31s) 
- **Session Status**: Shows COMPLETED, FAILED, ONGOING, or INTERRUPTED status
- **Error Details**: Displays specific error messages and exit codes
- **No Duplicates**: Filters out artificial auto-completed sessions from grap.py
- **Direct Log Parsing**: Bypasses grap.py limitations by reading raw log files

### Timeline Output Format
```
================================================================================
SST SESSIONS TIMELINE
================================================================================

Total SST Sessions: 4
----------------------------------------

[1] SST Session
    ├─ Start:  2025-09-15 13:45:56
    ├─ Status: FAILED
    ├─ Donor:  UAT-DB-03
    ├─ Joiner: UAT-DB-01
    ├─ Method: mariabackup
    ├─ Duration: 2m 15.0s
    └─ End:    2025-09-15 13:48:11
       Error:  Exit code 32: Broken pipe
       Events: 2 log entries

[2] SST Session
    ├─ Start:  2025-09-15 13:48:20
    ├─ Status: ONGOING
    ├─ Donor:  UAT-DB-03
    ├─ Joiner: UAT-DB-01
    ├─ Method: mariabackup
    └─ Status: Session still ongoing (incomplete)
       Events: 1 log entries
```

### Session Status Types
- **COMPLETED**: SST finished successfully
- **FAILED**: SST completed with errors (shows error details)
- **ONGOING**: SST request found but no completion event
- **INTERRUPTED**: SST was interrupted by a new SST request

## Output Format

The tool provides a structured summary including:

### 📊 Overview
- Analysis timestamp
- Total entities extracted
- Log timespan and duration
- Entity type breakdown

### 🔄 SST Sessions
- Total SST sessions count
- Sessions grouped by status (failed, completed, interrupted, etc.)
- Sessions grouped by method (mariabackup, rsync, etc.)
- Duration statistics (min, max, average, total)

### 📈 Performance Metrics
- SST success rate percentage
- Failed session count
- Average SST duration

### ⚠️ Issues Summary
- Error count and details
- Warning count (interrupted sessions, etc.)

### 🕒 Timeline
- Chronological view of recent events
- Property changes for temporal entities
- SST session state transitions

## Examples

### Example 1: Healthy Cluster
```bash
$ python3 graa.py healthy-cluster.log

================================================================================
GALERA LOG ANALYSIS SUMMARY
================================================================================

📊 OVERVIEW
   Analysis Time: 2025-09-22T13:32:31.477831
   Total Entities: 5
   Log Timespan: 2025-09-19T10:15:00 to 2025-09-19T10:45:00
   Duration: 0:30:00
   Entity Types:
     STATE_TRANSFER: 3
     CLUSTER_VIEW: 2

🔄 SST SESSIONS
   Total Sessions: 3
   By Status:
     completed: 3
   By Method:
     mariabackup: 3
   Duration Statistics:
     Count: 3
     Average: 5.2m
     Min: 3.1m
     Max: 8.5m
     Total: 15.6m

📈 PERFORMANCE
   SST Success Rate: 100.0% (3/3)
   Average SST Duration: 5.2m
```

### Example 2: Problematic Cluster
```bash
$ python3 graa.py problematic-cluster.log

================================================================================
GALERA LOG ANALYSIS SUMMARY
================================================================================

📊 OVERVIEW
   Analysis Time: 2025-09-22T13:32:44.662926
   Total Entities: 12
   Log Timespan: 2025-09-15T13:45:56 to 2025-09-22T13:32:44
   Duration: 6 days, 23:46:48
   Entity Types:
     STATE_TRANSFER: 12

🔄 SST SESSIONS
   Total Sessions: 12
   By Status:
     auto_completed_interrupted_by_new_sst: 8
     started: 4
   By Method:
     mariabackup: 8
     None: 4

📈 PERFORMANCE
   SST Success Rate: 66.7% (8/12)
   Failed Sessions: 4
   Average SST Duration: 74.5h

⚠️  ISSUES
   Warnings: 8
     sst_1: Session was auto_completed_interrupted_by_new_sst
     sst_2: Session was auto_completed_interrupted_by_new_sst
     ...
```

## Integration with Other Tools

### Export Analysis Data
```bash
# Export grap data for further processing
python3 grap.py --format=json galera-node.log > analysis.json

# Analyze the exported data
python3 graa.py --stdin < analysis.json
```

### Combine with Other Analysis
```bash
# Use in scripts for automated monitoring
python3 graa.py galera-node.log | grep "Success Rate"
python3 graa.py galera-node.log | grep "Failed Sessions"
```

## Dependencies

- Python 3.6+
- `grap.py` (for entity extraction)
- Standard library modules only (json, subprocess, pathlib, etc.)

## Features

- **Temporal Entity Analysis**: Leverages the temporal SST session management system
- **SST Timeline**: Detailed chronological view of SST sessions with real node names and accurate timing
- **Multi-version Support**: Works with MariaDB 10.6, 11.4, and other Galera versions
- **Pipeline Friendly**: Can be used in command pipelines
- **Human Readable**: Clean, structured output with emojis and formatting
- **Performance Focused**: Provides actionable metrics for cluster health assessment
- **Direct Log Parsing**: Bypasses grap.py limitations for accurate SST session analysis
