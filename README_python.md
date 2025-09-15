# Grambo Python Edition

A modern Python rewrite of the Grambo tool for analyzing MySQL/MariaDB Galera cluster log files.

## Overview

Grambo Python Edition provides a clean, organized analysis of Galera cluster logs with separate sections for different types of events. This makes it much easier to understand what's happening in your Galera cluster compared to the verbose output of the original bash version.

## Features

### 🎯 **Organized Event Categories**
- **📊 Server Information** - Version, socket, port, configuration
- **🔗 Galera Cluster Info** - Node UUID, group UUID, cluster details
- **🔄 State Transitions** - Node state changes (JOINED→SYNCED, etc.)
- **👥 Cluster Views** - Membership changes, nodes joining/leaving
- **💾 SST Events** - State Snapshot Transfer operations
- **📈 IST Events** - Incremental State Transfer operations
- **⚠️ Communication Issues** - Node suspicions, network problems
- **⚡ Warnings** - Non-critical issues
- **🚨 Errors** - Critical problems

### 🔧 **Advanced Features**
- **Multiple output formats**: Human-readable text and JSON
- **Event filtering**: Focus on specific event types
- **Timeline analysis**: Chronological view of cluster events
- **Cross-node analysis**: Analyze multiple log files together
- **Structured data**: Machine-readable JSON for integration

## Installation

```bash
# Clone the repository
git clone https://github.com/claudionanni/grambo.git
cd grambo

# Make the Python script executable (optional)
chmod +x grambo.py
```

## Usage

### Basic Usage

```bash
# Analyze a log file
python3 grambo.py /var/log/mysql/error.log

# Using stdin
cat /var/log/mysql/error.log | python3 grambo.py

# Make it executable and use directly
./grambo.py /var/log/mysql/error.log
```

### Advanced Options

```bash
# JSON output for integration with other tools
python3 grambo.py --format=json error.log

# Filter specific event types
python3 grambo.py --filter=sst_event,state_transition error.log

# Filter multiple types (comma-separated)
python3 grambo.py --filter=error,warning error.log

# Combine options
python3 grambo.py --format=json --filter=cluster_view error.log
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

### Text Format (Default)
```
================================================================================
| G R A M B O - Galera Log Deforester (Python Edition)
================================================================================

📊 SERVER INFORMATION
--------------------------------------------------
  Version: 10.6.16-MariaDB-0ubuntu0.22.04.1
  Socket: /run/mysqld/mysqld.sock
  Port: 3306
  Address: 10.0.1.193

🔗 GALERA CLUSTER INFORMATION
--------------------------------------------------
  Galera Version: 26.4.23
  Node UUID: 378c0ec7-9236-11f0-a3db-f6fdc24ecc7d
  Group UUID: 378cdc73-9236-11f0-a8d4-426872f4d003
  Group Name: my_wsrep_cluster

🔄 STATE TRANSITIONS
--------------------------------------------------
  2025-09-15 13:50:35 | JOINED → SYNCED
    └─ Sequence: 1776

💾 STATE SNAPSHOT TRANSFER (SST)
--------------------------------------------------
  2025-09-15 13:45:56 | SST REQUEST
    └─ Method: mariabackup
    └─ Donor: 10.0.1.192
    └─ Joiner: 10.0.1.193
  2025-09-15 13:48:11 | SST FAILED
```

### JSON Format
```json
{
  "server_info": {
    "version": "10.6.16-MariaDB-0ubuntu0.22.04.1",
    "socket": "/run/mysqld/mysqld.sock",
    "port": "3306",
    "address": "10.0.1.193"
  },
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
- **SST (State Snapshot Transfer)**: Full database backup/restore for new nodes
- **IST (Incremental State Transfer)**: Incremental catch-up using write-set cache

### Cluster Views
Track which nodes are members of the cluster at any given time, including:
- Nodes that joined the cluster
- Nodes that left gracefully  
- Nodes that were partitioned (network split)

## Legacy Compatibility

The original bash grambo is still available as `grambo` (without .py extension). The Python version provides the same analysis with much better organization and additional features.

## Requirements

- Python 3.7 or higher
- No external dependencies required

## Development

The code is organized into clear classes and functions:
- `GaleraLogParser`: Main parsing logic
- `LogEvent`: Data structure for parsed events  
- Event-specific parsers for each type of Galera event
- Modular regex patterns for easy maintenance

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample Galera logs
5. Submit a pull request

## License

Same license as the original grambo project.
